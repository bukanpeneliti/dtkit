#![allow(clippy::too_many_arguments)]

use glob::glob;
use polars::datatypes::{AnyValue, TimeUnit};
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

use crate::downcast::apply_cast;
use crate::mapping::FieldSpec;
use crate::sql_from_if::convert_if_sql;
use crate::stata_interface::{
    publish_transfer_metrics, read_macro, replace_number, replace_string, reset_transfer_metrics,
    set_macro, ST_retcode,
};
use crate::utilities::{
    compute_pool_init_count, determine_parallelization_strategy, get_compute_thread_pool,
    get_io_thread_pool, io_pool_init_count, warm_thread_pools, BatchMode, STATA_DATE_ORIGIN,
    STATA_EPOCH_MS, TIME_MS, TIME_NS, TIME_US,
};

#[allow(dead_code)]
const SCHEMA_VALIDATION_SAMPLE_ROWS: usize = 100;

#[allow(dead_code)]
pub fn validate_parquet_schema(path: &str, expected_columns: &[&str]) -> Result<(), String> {
    let file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let mut reader = ParquetReader::new(file);
    let schema = reader
        .schema()
        .map_err(|e| format!("Failed to read schema: {:?}", e))?;

    let parquet_columns: HashSet<&str> = schema.iter_names().map(|s| s.as_str()).collect();

    let missing: Vec<&str> = expected_columns
        .iter()
        .filter(|col| !parquet_columns.contains(*col))
        .copied()
        .collect();

    if !missing.is_empty() {
        return Err(format!("Missing columns in parquet file: {:?}", missing));
    }

    Ok(())
}

#[allow(dead_code)]
fn sample_parquet_schema(path: &str) -> Result<Schema, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = ParquetReader::new(file);

    let sample_df = reader
        .with_slice(Some((0, SCHEMA_VALIDATION_SAMPLE_ROWS)))
        .finish()
        .map_err(|e| format!("Failed to read sample: {:?}", e))?;

    Ok(sample_df.schema().as_ref().clone())
}

pub fn verify_parquet_path(path: &str) -> bool {
    let path_obj = Path::new(path);
    if path_obj.exists() && path_obj.is_file() {
        return true;
    }
    if path_obj.exists() && path_obj.is_dir() {
        return has_parquet_files_in_hive_structure(path);
    }
    if path.contains('*') || path.contains('?') || path.contains('[') {
        let normalized_pattern = if cfg!(windows) {
            path.replace('\\', "/")
        } else {
            path.to_string()
        };
        return glob(&normalized_pattern)
            .map(|p| p.filter_map(Result::ok).next().is_some())
            .unwrap_or(false);
    }
    false
}

fn has_parquet_files_in_hive_structure(dir_path: &str) -> bool {
    let dir = Path::new(dir_path);
    if !dir.is_dir() {
        return false;
    }

    for entry in WalkDir::new(dir)
        .max_depth(3)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext.eq_ignore_ascii_case("parquet") {
                    return true;
                }
            }
        }
    }
    false
}

pub fn has_metadata_key(path: &str, key: &str) -> Result<bool, Box<dyn Error>> {
    let bytes = std::fs::read(path)?;
    Ok(bytes
        .windows(key.len())
        .any(|window| window == key.as_bytes()))
}

pub fn open_parquet_scan(
    path: &str,
    safe_relaxed: bool,
    asterisk_to_variable_name: Option<&str>,
) -> Result<LazyFrame, PolarsError> {
    let path_obj = Path::new(path);
    if path_obj.is_dir() {
        return scan_hive_partitioned(path);
    }

    match (safe_relaxed, asterisk_to_variable_name) {
        (_, Some(var_name)) => scan_with_filename_extraction(path, var_name),
        (true, _) => scan_with_diagonal_relaxed(path),
        _ => {
            let mut normalized_pattern = if cfg!(windows) {
                path.replace('\\', "/")
            } else {
                path.to_string()
            };
            if normalized_pattern.contains("**.") {
                normalized_pattern = normalized_pattern.replace("**.", "**/*.");
            }
            let scan_args = ScanArgsParquet {
                allow_missing_columns: true,
                cache: false,
                ..Default::default()
            };
            LazyFrame::scan_parquet(PlPath::new(&normalized_pattern), scan_args)
        }
    }
}

fn scan_hive_partitioned(dir_path: &str) -> Result<LazyFrame, PolarsError> {
    let mut glob_pattern = String::from(dir_path);
    if glob_pattern.ends_with('/') {
        glob_pattern.pop();
    }
    if cfg!(windows) {
        glob_pattern = glob_pattern.replace('\\', "/");
    }
    let test_patterns = vec![
        format!("{}/**/*.parquet", glob_pattern),
        format!("{}/*/*.parquet", glob_pattern),
        format!("{}/*/*/*.parquet", glob_pattern),
    ];
    for pattern in test_patterns {
        if let Ok(paths) = glob(&pattern) {
            let files: Vec<_> = paths.filter_map(Result::ok).collect();
            if !files.is_empty() {
                let scan_args = ScanArgsParquet {
                    allow_missing_columns: true,
                    cache: false,
                    ..Default::default()
                };
                return LazyFrame::scan_parquet(PlPath::new(&pattern), scan_args);
            }
        }
    }
    Err(PolarsError::ComputeError(
        format!(
            "No parquet files found in hive partitioned structure: {}",
            dir_path
        )
        .into(),
    ))
}

fn scan_with_diagonal_relaxed(glob_path: &str) -> Result<LazyFrame, PolarsError> {
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    let paths = glob(&normalized_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid glob pattern: {}", e).into()))?;
    let file_paths: Result<Vec<PathBuf>, _> = paths.collect();
    let file_paths = file_paths.map_err(|e| {
        PolarsError::ComputeError(format!("Failed to read glob results: {}", e).into())
    })?;
    if file_paths.is_empty() {
        return Err(PolarsError::ComputeError(
            format!("No files found matching pattern: {}", normalized_pattern).into(),
        ));
    }

    let scan_args = ScanArgsParquet {
        allow_missing_columns: true,
        cache: false,
        ..Default::default()
    };
    let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|path| {
            let path_string = path.to_string_lossy().to_string();
            LazyFrame::scan_parquet(PlPath::new(&path_string), scan_args.clone())
        })
        .collect();

    concat(
        lazy_frames?,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: true,
            diagonal: true,
            from_partitioned_ds: true,
            maintain_order: true,
        },
    )
}

fn scan_with_filename_extraction(
    glob_path: &str,
    variable_name: &str,
) -> Result<LazyFrame, PolarsError> {
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    let asterisk_pos = normalized_pattern
        .find('*')
        .ok_or_else(|| PolarsError::ComputeError("No asterisk found in glob pattern".into()))?;
    let before_asterisk = &normalized_pattern[..asterisk_pos];
    let after_asterisk = &normalized_pattern[asterisk_pos + 1..];
    let regex_pattern = format!(
        "{}(.+?){}",
        regex::escape(before_asterisk),
        regex::escape(after_asterisk)
    );
    let re = Regex::new(&regex_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid regex pattern: {}", e).into()))?;

    let paths = glob(&normalized_pattern)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid glob pattern: {}", e).into()))?;
    let file_paths: Result<Vec<PathBuf>, _> = paths.collect();
    let file_paths = file_paths.map_err(|e| {
        PolarsError::ComputeError(format!("Failed to read glob results: {}", e).into())
    })?;
    if file_paths.is_empty() {
        return Err(PolarsError::ComputeError(
            format!("No files found matching pattern: {}", normalized_pattern).into(),
        ));
    }

    let scan_args = ScanArgsParquet {
        allow_missing_columns: true,
        cache: false,
        ..Default::default()
    };
    let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|path| {
            let path_str = path.to_string_lossy();
            let normalized_path = if cfg!(windows) {
                path_str.replace('\\', "/")
            } else {
                path_str.to_string()
            };
            let extracted_value = re
                .captures(&normalized_path)
                .and_then(|caps| caps.get(1))
                .map(|m| m.as_str())
                .unwrap_or("unknown");
            LazyFrame::scan_parquet(PlPath::new(path_str.as_ref()), scan_args.clone())
                .map(|lf| lf.with_columns([smart_lit(extracted_value).alias(variable_name)]))
        })
        .collect();

    concat(
        lazy_frames?,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: true,
            diagonal: true,
            from_partitioned_ds: true,
            maintain_order: true,
        },
    )
}

fn smart_lit(value: &str) -> Expr {
    let trimmed = value.trim();
    if let Ok(int_val) = trimmed.parse::<i64>() {
        return lit(int_val);
    }
    if let Ok(float_val) = trimmed.parse::<f64>() {
        return lit(float_val);
    }
    lit(value)
}

fn publish_read_runtime_metrics(
    collect_calls: usize,
    planned_batches: usize,
    processed_batches: usize,
    elapsed_ms: u128,
) {
    set_macro("dtpq_read_collect_calls", &collect_calls.to_string(), true);
    set_macro(
        "dtpq_read_planned_batches",
        &planned_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_read_processed_batches",
        &processed_batches.to_string(),
        true,
    );
    set_macro("dtpq_read_elapsed_ms", &elapsed_ms.to_string(), true);
    set_macro(
        "dtpq_compute_pool_threads",
        &get_compute_thread_pool().current_num_threads().to_string(),
        true,
    );
    set_macro(
        "dtpq_compute_pool_inits",
        &compute_pool_init_count().to_string(),
        true,
    );
    set_macro(
        "dtpq_io_pool_threads",
        &get_io_thread_pool().current_num_threads().to_string(),
        true,
    );
    set_macro(
        "dtpq_io_pool_inits",
        &io_pool_init_count().to_string(),
        true,
    );
    publish_transfer_metrics("dtpq_read");
}

pub fn import_parquet(
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    parallel_strategy: Option<BatchMode>,
    safe_relaxed: bool,
    asterisk_var: Option<&str>,
    sort: &str,
    stata_offset: usize,
    random_share: f64,
    random_seed: u64,
    batch_size: usize,
) -> Result<i32, Box<dyn Error>> {
    let started_at = Instant::now();
    let mut collect_calls = 0usize;
    let mut processed_batches = 0usize;
    warm_thread_pools();
    reset_transfer_metrics();
    publish_read_runtime_metrics(0, 0, 0, 0);

    let variables_owned;
    let variables_as_str = if variables_as_str.is_empty() || variables_as_str == "from_macro" {
        variables_owned = read_macro("matched_vars", false, Some(10 * 1024 * 1024));
        variables_owned.as_str()
    } else {
        variables_as_str
    };

    let all_columns_unfiltered: Vec<FieldSpec> = if mapping.is_empty() || mapping == "from_macros" {
        let n_vars = read_macro("n_matched_vars", false, None)
            .parse::<usize>()
            .unwrap_or(0);
        column_info_from_macros(n_vars)
    } else {
        serde_json::from_str(mapping).unwrap_or_default()
    };

    let selected_column_list: Vec<&str> = variables_as_str.split_whitespace().collect();
    let selected_column_names: HashSet<&str> = selected_column_list.iter().copied().collect();
    let all_columns: Vec<FieldSpec> = all_columns_unfiltered
        .into_iter()
        .filter(|col_info| selected_column_names.contains(col_info.name.as_str()))
        .collect();

    let can_use_eager = Path::new(path).is_file()
        && !path.contains('*')
        && !path.contains('?')
        && !safe_relaxed
        && asterisk_var.is_none()
        && sql_if.map(|s| s.trim().is_empty()).unwrap_or(true)
        && sort.trim().is_empty()
        && random_share <= 0.0;

    if can_use_eager {
        if !selected_column_list.is_empty() {
            if let Err(e) = validate_parquet_schema(path, &selected_column_list) {
                crate::stata_interface::display(&format!("Schema validation warning: {}", e));
            }
        }

        let file = File::open(path)?;
        let mut df = ParquetReader::new(file)
            .with_slice(Some((offset, n_rows)))
            .finish()?;

        let columns_vec: Vec<PlSmallStr> = selected_column_list
            .iter()
            .map(|s| PlSmallStr::from(*s))
            .collect();
        df = df.select(columns_vec)?;

        let cast_json = read_macro("cast_json", false, None);
        if !cast_json.is_empty() {
            let cast_mapping: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&cast_json)?;
            if let Some(serde_json::Value::Array(cols)) = cast_mapping.get("string") {
                for col_value in cols {
                    if let serde_json::Value::String(col_name) = col_value {
                        if df.get_column_index(col_name).is_some() {
                            df.try_apply(col_name, |s| s.cast(&DataType::String))?;
                        }
                    }
                }
            }
        }

        let cat_like_cols: Vec<String> = df
            .schema()
            .iter()
            .filter_map(|(name, dtype)| {
                if matches!(dtype, DataType::Categorical(_, _) | DataType::Enum(_, _)) {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect();
        for col_name in cat_like_cols {
            df.try_apply(&col_name, |s| s.cast(&DataType::String))?;
        }

        let n_threads = get_compute_thread_pool().current_num_threads().max(1);
        let strategy = parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(selected_column_list.len(), df.height(), n_threads)
        });
        let total_rows = df.height();
        let n_batches = (total_rows as f64 / batch_size as f64).ceil() as usize;
        set_macro("n_batches", &n_batches.to_string(), false);

        let mut loaded_rows = 0usize;
        for batch_i in 0..n_batches {
            let batch_offset = batch_i * batch_size;
            let batch_length = if (batch_i + 1) * batch_size > total_rows {
                total_rows - batch_i * batch_size
            } else {
                batch_size
            };
            let batch_df = df.slice(batch_offset as i64, batch_length);
            loaded_rows += batch_df.height();
            process_batch_with_strategy(
                &batch_df,
                batch_offset,
                &all_columns,
                strategy,
                stata_offset,
            )?;
            processed_batches += 1;
        }
        set_macro("n_loaded_rows", &loaded_rows.to_string(), false);
        publish_read_runtime_metrics(
            collect_calls,
            n_batches,
            processed_batches,
            started_at.elapsed().as_millis(),
        );

        return Ok(0);
    }

    let mut lf = open_parquet_scan(path, safe_relaxed, asterisk_var)?;

    let cast_json = read_macro("cast_json", false, None);
    if !cast_json.is_empty() {
        lf = apply_cast(lf, &cast_json)?;
    }
    lf = normalize_categorical(&lf)?;

    let mut batch_source_offset = offset;
    if sql_if.map(|s| !s.trim().is_empty()).unwrap_or(false) {
        lf = lf.slice(offset as i64, n_rows as u32);
        batch_source_offset = 0;
    }

    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", lf);
        let translated = convert_if_sql(sql);
        lf = ctx.execute(&format!("select * from df where {}", translated))?;
    }

    if random_share > 0.0 {
        let random_seed_option = if random_seed == 0 {
            None
        } else {
            Some(random_seed)
        };
        collect_calls += 1;
        let sampled = lf.collect()?.sample_frac(
            &Series::new("frac".into(), vec![random_share]),
            false,
            false,
            random_seed_option,
        )?;
        lf = sampled.lazy();
    }

    if !sort.is_empty() {
        let mut sort_options = SortMultipleOptions::default();
        let mut sort_cols: Vec<PlSmallStr> = Vec::new();
        let mut descending: Vec<bool> = Vec::new();
        for token in sort.split_whitespace() {
            if token.starts_with('-') && token.len() > 1 {
                sort_cols.push(PlSmallStr::from(&token[1..]));
                descending.push(true);
            } else {
                sort_cols.push(PlSmallStr::from(token));
                descending.push(false);
            }
        }
        sort_options.descending = descending;
        lf = lf.sort(sort_cols, sort_options);
    }

    let use_streaming = n_rows > 1_000_000;

    let columns: Vec<Expr> = selected_column_list.iter().map(|s| col(*s)).collect();
    let n_batches = (n_rows as f64 / batch_size as f64).ceil() as usize;
    let n_threads = get_compute_thread_pool().current_num_threads().max(1);
    let strategy = parallel_strategy
        .unwrap_or_else(|| determine_parallelization_strategy(columns.len(), n_rows, n_threads));
    set_macro("n_batches", &n_batches.to_string(), false);

    let mut loaded_rows = 0usize;
    for batch_i in 0..n_batches {
        let mut lf_batch = lf.clone().select(&columns);
        let batch_offset = batch_source_offset + batch_i * batch_size;
        let batch_length = if (batch_i + 1) * batch_size > n_rows {
            n_rows - batch_i * batch_size
        } else {
            batch_size
        } as u32;
        lf_batch = lf_batch.slice(batch_offset as i64, batch_length);
        let batch_df = if use_streaming {
            collect_calls += 1;
            lf_batch.with_new_streaming(true).collect()?
        } else {
            collect_calls += 1;
            lf_batch.collect()?
        };
        if batch_df.height() == 0 {
            break;
        }
        loaded_rows += batch_df.height();
        process_batch_with_strategy(
            &batch_df,
            batch_offset - batch_source_offset,
            &all_columns,
            strategy,
            stata_offset,
        )?;
        processed_batches += 1;
    }
    set_macro("n_loaded_rows", &loaded_rows.to_string(), false);
    publish_read_runtime_metrics(
        collect_calls,
        n_batches,
        processed_batches,
        started_at.elapsed().as_millis(),
    );

    Ok(0)
}

fn set_schema_macros(
    schema: &Schema,
    string_lengths: &std::collections::HashMap<String, usize>,
    detailed: bool,
    quietly: bool,
) -> PolarsResult<usize> {
    if !quietly {
        crate::stata_interface::display(
            "Variable Name                    | Polars Type                      | Stata Type",
        );
        crate::stata_interface::display(
            "-------------------------------- | -------------------------------- | ----------",
        );
    }

    for (i, (name, dtype)) in schema.iter().enumerate() {
        let polars_type = match dtype {
            DataType::Boolean => "int8",
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "uint16",
            DataType::UInt32 => "uint32",
            DataType::UInt64 => "uint64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Date => "int32",
            DataType::Time => "int64",
            DataType::Datetime(_, _) => "int64",
            DataType::String => "string",
            DataType::Categorical(_, _) => "categorical",
            DataType::Enum(_, _) => "enum",
            DataType::Binary => "binary",
            _ => "string",
        };

        let is_string_like = matches!(
            dtype,
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _)
        );
        let string_length = if detailed {
            *string_lengths.get(name.as_str()).unwrap_or(&0)
        } else if is_string_like {
            2045
        } else {
            0
        };
        let stata_type = match dtype {
            DataType::Boolean => "byte",
            DataType::Int8 => "byte",
            DataType::Int16 => "int",
            DataType::Int32 => "long",
            DataType::Int64 => "double",
            DataType::UInt8 => "int",
            DataType::UInt16 => "long",
            DataType::UInt32 => "double",
            DataType::UInt64 => "double",
            DataType::Float32 => "float",
            DataType::Float64 => "double",
            DataType::Date => "date",
            DataType::Time => "time",
            DataType::Datetime(_, _) => "datetime",
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                if detailed && string_length > 2045 {
                    "strl"
                } else {
                    "string"
                }
            }
            DataType::Binary => "binary",
            _ => "strl",
        };

        if !quietly {
            crate::stata_interface::display(&format!(
                "{:<32} | {:<32} | {}",
                name,
                format!("{:?}", dtype),
                stata_type
            ));
        }

        set_macro(&format!("name_{}", i + 1), name, false);
        set_macro(&format!("type_{}", i + 1), stata_type, false);
        set_macro(&format!("polars_type_{}", i + 1), polars_type, false);
        set_macro(
            &format!("string_length_{}", i + 1),
            &string_length.to_string(),
            false,
        );
        set_macro(&format!("rename_{}", i + 1), "", false);
    }

    Ok(schema.len())
}

pub fn file_summary(
    path: &str,
    quietly: bool,
    detailed: bool,
    sql_if: Option<&str>,
    safe_relaxed: bool,
    asterisk_to_variable_name: Option<&str>,
    _compress: bool,
    _compress_string_to_numeric: bool,
) -> ST_retcode {
    set_macro("cast_json", "", false);

    let _ = sql_if;
    let _ = safe_relaxed;
    let _ = asterisk_to_variable_name;

    let file = match File::open(path) {
        Ok(v) => v,
        Err(e) => {
            crate::stata_interface::display(&format!("Error opening parquet file: {}", e));
            return 198;
        }
    };
    let df = match ParquetReader::new(file).finish() {
        Ok(v) => v,
        Err(e) => {
            crate::stata_interface::display(&format!("Error reading parquet file: {:?}", e));
            return 198;
        }
    };

    let schema = df.schema().as_ref().clone();
    let mut string_lengths = std::collections::HashMap::<String, usize>::new();
    if detailed {
        for (name, dtype) in schema.iter() {
            if matches!(
                dtype,
                DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _)
            ) {
                let len = df
                    .column(name.as_str())
                    .ok()
                    .and_then(|s| s.str().ok())
                    .and_then(|ca| {
                        ca.into_iter()
                            .map(|v| v.map(|x| x.len()).unwrap_or(0))
                            .max()
                    })
                    .unwrap_or(0);
                string_lengths.insert(name.to_string(), len);
            }
        }
    }

    let n_columns = match set_schema_macros(&schema, &string_lengths, detailed, quietly) {
        Ok(v) => v,
        Err(e) => {
            crate::stata_interface::display(&format!("Error building schema macros: {:?}", e));
            return 198;
        }
    };

    let n_rows = df.height();

    set_macro("n_columns", &n_columns.to_string(), false);
    set_macro("n_rows", &n_rows.to_string(), false);

    if !quietly {
        crate::stata_interface::display(&format!("n columns = {}", n_columns));
        crate::stata_interface::display(&format!("n rows = {}", n_rows));
    }

    0
}

pub fn normalize_categorical(lf: &LazyFrame) -> Result<LazyFrame, PolarsError> {
    let schema = lf.clone().collect_schema()?;
    let cat_expressions: Vec<Expr> = schema
        .iter()
        .filter_map(|(name, dtype)| {
            if matches!(dtype, DataType::Categorical(_, _) | DataType::Enum(_, _)) {
                Some(col(name.clone()).cast(DataType::String))
            } else {
                None
            }
        })
        .collect();
    if cat_expressions.is_empty() {
        Ok(lf.clone())
    } else {
        Ok(lf.clone().with_columns(cat_expressions))
    }
}

fn column_info_from_macros(n_vars: usize) -> Vec<FieldSpec> {
    let mut column_infos = Vec::with_capacity(n_vars);
    for i in 0..n_vars {
        let index = read_macro(&format!("v_to_read_index_{}", i + 1), false, None)
            .parse::<usize>()
            .unwrap_or(1)
            - 1;
        let name = read_macro(&format!("v_to_read_name_{}", i + 1), false, None);
        let dtype = read_macro(&format!("v_to_read_p_type_{}", i + 1), false, None);
        let stata_type =
            read_macro(&format!("v_to_read_type_{}", i + 1), false, None).to_lowercase();
        column_infos.push(FieldSpec {
            index,
            name,
            dtype,
            stata_type,
        });
    }
    column_infos
}

fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    all_columns: &Vec<FieldSpec>,
    strategy: BatchMode,
    stata_offset: usize,
) -> PolarsResult<()> {
    let row_count = batch.height();
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        return process_row_range(batch, start_index, 0, row_count, all_columns, stata_offset);
    }

    pool.install(|| match strategy {
        BatchMode::ByRow => {
            let n_workers = rayon::current_num_threads().max(1);
            let chunk_size = std::cmp::max(512, row_count.div_ceil(n_workers * 8));
            (0..row_count)
                .into_par_iter()
                .chunks(chunk_size)
                .try_for_each(|chunk| {
                    let start_row = chunk[0];
                    let end_row = chunk[chunk.len() - 1] + 1;
                    process_row_range(
                        batch,
                        start_index,
                        start_row,
                        end_row,
                        all_columns,
                        stata_offset,
                    )
                })
        }
        BatchMode::ByColumn => all_columns.par_iter().try_for_each(|col_info| {
            let col = batch.column(&col_info.name)?;
            for row_idx in 0..row_count {
                let global_row_idx = row_idx + start_index;
                assign_cell(col, col_info, row_idx, global_row_idx, stata_offset)?;
            }
            Ok(())
        }),
    })
}

fn process_row_range(
    batch: &DataFrame,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    all_columns: &Vec<FieldSpec>,
    stata_offset: usize,
) -> PolarsResult<()> {
    for col_info in all_columns {
        let col = batch.column(&col_info.name)?;
        for row_idx in start_row..end_row {
            let global_row_idx = row_idx + start_index;
            assign_cell(col, col_info, row_idx, global_row_idx, stata_offset)?;
        }
    }
    Ok(())
}

fn assign_cell(
    col: &Column,
    col_info: &FieldSpec,
    row_idx: usize,
    global_row_idx: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col_info.stata_type.as_str() {
        "string" | "strl" => {
            let out = match col.get(row_idx) {
                Ok(AnyValue::String(v)) => Some(v.to_string()),
                Ok(AnyValue::StringOwned(v)) => Some(v.to_string()),
                Ok(AnyValue::Null) => None,
                Ok(v) => Some(v.to_string()),
                Err(_) => None,
            };
            replace_string(out, global_row_idx + 1 + stata_offset, col_info.index + 1);
            Ok(())
        }
        "datetime" => {
            let mills_factor = match col.dtype() {
                DataType::Datetime(TimeUnit::Nanoseconds, _) => (TIME_NS / TIME_MS) as f64,
                DataType::Datetime(TimeUnit::Microseconds, _) => (TIME_US / TIME_MS) as f64,
                DataType::Datetime(TimeUnit::Milliseconds, _) => 1.0,
                _ => 1.0,
            };
            let sec_shift_scaled = (STATA_EPOCH_MS as f64) * (TIME_MS as f64);
            let value = match col.get(row_idx) {
                Ok(AnyValue::Datetime(v, _, _)) => Some(v as f64 / mills_factor + sec_shift_scaled),
                _ => None,
            };
            replace_number(value, global_row_idx + 1 + stata_offset, col_info.index + 1);
            Ok(())
        }
        _ => {
            let value = match col.get(row_idx) {
                Ok(AnyValue::Boolean(v)) => Some(if v { 1.0 } else { 0.0 }),
                Ok(AnyValue::Int8(v)) => Some(v as f64),
                Ok(AnyValue::Int16(v)) => Some(v as f64),
                Ok(AnyValue::Int32(v)) => Some(v as f64),
                Ok(AnyValue::Int64(v)) => Some(v as f64),
                Ok(AnyValue::UInt8(v)) => Some(v as f64),
                Ok(AnyValue::UInt16(v)) => Some(v as f64),
                Ok(AnyValue::UInt32(v)) => Some(v as f64),
                Ok(AnyValue::UInt64(v)) => Some(v as f64),
                Ok(AnyValue::Float32(v)) => Some(v as f64),
                Ok(AnyValue::Float64(v)) => Some(v),
                Ok(AnyValue::Date(v)) => Some((v + STATA_DATE_ORIGIN) as f64),
                Ok(AnyValue::Time(v)) => Some((v / TIME_US) as f64),
                _ => None,
            };
            replace_number(value, global_row_idx + 1 + stata_offset, col_info.index + 1);
            Ok(())
        }
    }
}
