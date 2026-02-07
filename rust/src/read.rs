use glob::glob;
use polars::datatypes::{AnyValue, TimeUnit};
use polars::error::ErrString;
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::downcast::apply_cast;
use crate::mapping::ColumnInfo;
use crate::stata_interface::{get_macro, replace_number, replace_string, set_macro, ST_retcode};
use crate::utilities::{
    determine_parallelization_strategy, ParallelizationStrategy, DAY_SHIFT_SAS_STATA,
    SEC_MICROSECOND, SEC_MILLISECOND, SEC_NANOSECOND, SEC_SHIFT_SAS_STATA,
};

pub fn data_exists(path: &str) -> bool {
    let path_obj = Path::new(path);
    if path_obj.exists() && path_obj.is_file() {
        return true;
    }
    if path_obj.exists() && path_obj.is_dir() {
        return has_parquet_files_in_hive_structure(path);
    }
    is_valid_glob_pattern(path)
}

fn has_parquet_files_in_hive_structure(dir_path: &str) -> bool {
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
        format!("{}/*.parquet", glob_pattern),
    ];
    for pattern in test_patterns {
        if let Ok(mut paths) = glob(&pattern) {
            if paths.next().is_some() {
                return true;
            }
        }
    }
    false
}

fn is_valid_glob_pattern(glob_path: &str) -> bool {
    if !glob_path.contains('*') && !glob_path.contains('?') && !glob_path.contains('[') {
        return false;
    }
    let mut normalized_pattern = if cfg!(windows) {
        glob_path.replace('\\', "/")
    } else {
        glob_path.to_string()
    };
    if normalized_pattern.contains("**.") {
        normalized_pattern = normalized_pattern.replace("**.", "**/*.");
    }
    match glob(&normalized_pattern) {
        Ok(paths) => paths.filter_map(Result::ok).next().is_some(),
        Err(_) => false,
    }
}

pub fn has_metadata_key(path: &str, key: &str) -> Result<bool, Box<dyn Error>> {
    let bytes = std::fs::read(path)?;
    Ok(bytes
        .windows(key.len())
        .any(|window| window == key.as_bytes()))
}

pub fn scan_lazyframe(
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
            let mut scan_args = ScanArgsParquet::default();
            scan_args.allow_missing_columns = true;
            scan_args.cache = false;
            LazyFrame::scan_parquet(&normalized_pattern, scan_args)
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
                let mut scan_args = ScanArgsParquet::default();
                scan_args.allow_missing_columns = true;
                scan_args.cache = false;
                return LazyFrame::scan_parquet(&pattern, scan_args);
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

    let mut scan_args = ScanArgsParquet::default();
    scan_args.allow_missing_columns = true;
    scan_args.cache = false;
    let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|path| LazyFrame::scan_parquet(path.to_string_lossy().as_ref(), scan_args.clone()))
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

    let mut scan_args = ScanArgsParquet::default();
    scan_args.allow_missing_columns = true;
    scan_args.cache = false;
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
            LazyFrame::scan_parquet(path_str.as_ref(), scan_args.clone())
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

pub fn read_to_stata(
    path: &str,
    variables_as_str: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    parallel_strategy: Option<ParallelizationStrategy>,
    safe_relaxed: bool,
    asterisk_var: Option<&str>,
    sort: &str,
    stata_offset: usize,
    random_share: f64,
    random_seed: u64,
    batch_size: usize,
) -> Result<i32, Box<dyn Error>> {
    let variables_owned;
    let variables_as_str = if variables_as_str.is_empty() || variables_as_str == "from_macro" {
        variables_owned = get_macro("matched_vars", false, Some(10 * 1024 * 1024));
        variables_owned.as_str()
    } else {
        variables_as_str
    };

    let all_columns_unfiltered: Vec<ColumnInfo> = if mapping.is_empty() || mapping == "from_macros"
    {
        let n_vars = get_macro("n_matched_vars", false, None)
            .parse::<usize>()
            .unwrap_or(0);
        column_info_from_macros(n_vars)
    } else {
        serde_json::from_str(mapping).unwrap_or_default()
    };

    let selected_column_list: Vec<&str> = variables_as_str.split_whitespace().collect();
    let selected_column_names: HashSet<&str> = selected_column_list.iter().copied().collect();
    let all_columns: Vec<ColumnInfo> = all_columns_unfiltered
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
        let file = File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;

        let cast_json = get_macro("cast_json", false, None);
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

        let columns_vec: Vec<PlSmallStr> = selected_column_list
            .iter()
            .map(|s| PlSmallStr::from(*s))
            .collect();
        df = df.select(columns_vec)?;

        let sliced = df.slice(offset as i64, n_rows);
        let n_threads = 1;
        let strategy = parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(selected_column_list.len(), n_rows, n_threads)
        });
        let n_batches = (n_rows as f64 / batch_size as f64).ceil() as usize;
        set_macro("n_batches", &n_batches.to_string(), false);

        for batch_i in 0..n_batches {
            let batch_offset = batch_i * batch_size;
            let batch_length = if (batch_i + 1) * batch_size > n_rows {
                n_rows - batch_i * batch_size
            } else {
                batch_size
            };
            let batch_df = sliced.slice(batch_offset as i64, batch_length);
            process_batch_with_strategy(
                &batch_df,
                batch_offset,
                &all_columns,
                strategy,
                n_threads,
                stata_offset,
            )?;
        }

        return Ok(0);
    }

    let mut lf = scan_lazyframe(path, safe_relaxed, asterisk_var)?;

    let cast_json = get_macro("cast_json", false, None);
    if !cast_json.is_empty() {
        lf = apply_cast(lf, &cast_json)?;
    }
    lf = cast_catenum_to_string(&lf)?;

    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", lf);
        lf = ctx.execute(&format!("select * from df where {}", sql))?;
    }

    if random_share > 0.0 {
        let random_seed_option = if random_seed == 0 {
            None
        } else {
            Some(random_seed)
        };
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

    let columns: Vec<Expr> = selected_column_list.iter().map(|s| col(*s)).collect();
    let n_batches = (n_rows as f64 / batch_size as f64).ceil() as usize;
    let n_threads = 1;
    let strategy = parallel_strategy
        .unwrap_or_else(|| determine_parallelization_strategy(columns.len(), n_rows, n_threads));
    set_macro("n_batches", &n_batches.to_string(), false);

    for batch_i in 0..n_batches {
        let mut lf_batch = lf.clone().select(&columns);
        let batch_offset = offset + batch_i * batch_size;
        let batch_length = if (batch_i + 1) * batch_size > n_rows {
            n_rows - batch_i * batch_size
        } else {
            batch_size
        } as u32;
        lf_batch = lf_batch.slice(batch_offset as i64, batch_length);
        let batch_df = lf_batch.collect()?;
        process_batch_with_strategy(
            &batch_df,
            batch_offset - offset,
            &all_columns,
            strategy,
            n_threads,
            stata_offset,
        )?;
    }

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
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => "string",
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
                if !detailed {
                    "strl"
                } else if string_length > 2045 {
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

pub fn cast_catenum_to_string(lf: &LazyFrame) -> Result<LazyFrame, PolarsError> {
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

fn column_info_from_macros(n_vars: usize) -> Vec<ColumnInfo> {
    let mut column_infos = Vec::with_capacity(n_vars);
    for i in 0..n_vars {
        let index = get_macro(&format!("v_to_read_index_{}", i + 1), false, None)
            .parse::<usize>()
            .unwrap_or(1)
            - 1;
        let name = get_macro(&format!("v_to_read_name_{}", i + 1), false, None);
        let dtype = get_macro(&format!("v_to_read_p_type_{}", i + 1), false, None);
        let stata_type = get_macro(&format!("v_to_read_type_{}", i + 1), false, None);
        column_infos.push(ColumnInfo {
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
    all_columns: &Vec<ColumnInfo>,
    strategy: ParallelizationStrategy,
    n_threads: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    let row_count = batch.height();
    if n_threads <= 1 || row_count < 10_000 {
        return process_row_range(batch, start_index, 0, row_count, all_columns, stata_offset);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .map_err(|e| {
            PolarsError::ComputeError(ErrString::from(format!(
                "Failed to build thread pool: {}",
                e
            )))
        })?;

    pool.install(|| match strategy {
        ParallelizationStrategy::ByRow => {
            let chunk_size = std::cmp::max(100, row_count / (rayon::current_num_threads() * 4));
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
        ParallelizationStrategy::ByColumn => all_columns.par_iter().try_for_each(|col_info| {
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
    all_columns: &Vec<ColumnInfo>,
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
    col_info: &ColumnInfo,
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
                DataType::Datetime(TimeUnit::Nanoseconds, _) => {
                    (SEC_NANOSECOND / SEC_MILLISECOND) as f64
                }
                DataType::Datetime(TimeUnit::Microseconds, _) => {
                    (SEC_MICROSECOND / SEC_MILLISECOND) as f64
                }
                DataType::Datetime(TimeUnit::Milliseconds, _) => 1.0,
                _ => 1.0,
            };
            let sec_shift_scaled = (SEC_SHIFT_SAS_STATA as f64) * (SEC_MILLISECOND as f64);
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
                Ok(AnyValue::Date(v)) => Some((v + DAY_SHIFT_SAS_STATA) as f64),
                Ok(AnyValue::Time(v)) => Some((v / SEC_MICROSECOND) as f64),
                _ => None,
            };
            replace_number(value, global_row_idx + 1 + stata_offset, col_info.index + 1);
            Ok(())
        }
    }
}
