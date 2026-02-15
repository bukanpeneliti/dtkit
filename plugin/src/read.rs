#![allow(clippy::too_many_arguments)]

use glob::glob;
use polars::datatypes::{AnyValue, TimeUnit};
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

use crate::downcast::apply_cast;
use crate::if_filter::{compile_if_expr, convert_if_sql};
use crate::mapping::FieldSpec;
use crate::stata_interface::{
    publish_transfer_metrics, read_macro, record_transfer_conversion_failure,
    record_transfer_fallback, replace_number, replace_string, reset_transfer_metrics, set_macro,
    ST_retcode,
};
use crate::utilities::{
    compute_pool_init_count, determine_parallelization_strategy, get_compute_thread_pool,
    get_io_thread_pool, io_pool_init_count, warm_thread_pools, AdaptiveBatchTuner, BatchMode,
    STATA_DATE_ORIGIN, STATA_EPOCH_MS, TIME_MS, TIME_NS, TIME_US,
};

#[allow(dead_code)]
const SCHEMA_VALIDATION_SAMPLE_ROWS: usize = 100;
const ENV_METADATA_LOOKUP_MODE: &str = "DTPARQUET_METADATA_LOOKUP_MODE";
const ENV_LAZY_EXECUTION_MODE: &str = "DTPARQUET_LAZY_EXECUTION_MODE";

#[derive(Debug, Deserialize)]
struct SchemaHandoff<T> {
    #[serde(alias = "v")]
    protocol_version: u32,
    #[serde(alias = "f")]
    fields: Vec<T>,
}

#[derive(Debug, Serialize)]
struct DescribeFieldPayload {
    #[serde(rename = "n")]
    name: String,
    #[serde(rename = "s")]
    stata_type: String,
    #[serde(rename = "p")]
    polars_type: String,
    #[serde(rename = "l")]
    string_length: usize,
    #[serde(rename = "r")]
    rename: String,
}

#[derive(Debug, Serialize)]
struct DescribeSchemaPayload {
    #[serde(rename = "v")]
    protocol_version: u32,
    #[serde(rename = "f")]
    fields: Vec<DescribeFieldPayload>,
}

#[derive(Debug)]
struct ReadScanPlan {
    selected_column_list: Vec<String>,
    transfer_columns: Vec<TransferColumnSpec>,
    can_use_eager: bool,
}

fn parse_schema_handoff_fields<T: DeserializeOwned>(
    mapping: &str,
    handoff_name: &str,
) -> Result<(Vec<T>, &'static str), Box<dyn Error>> {
    if let Ok(payload) = serde_json::from_str::<SchemaHandoff<T>>(mapping) {
        if payload.protocol_version != crate::SCHEMA_HANDOFF_PROTOCOL_VERSION {
            return Err(format!(
                "Schema protocol mismatch for {}: expected version {}, got {}. Update ado and plugin to matching versions or retry with legacy macro handoff.",
                handoff_name,
                crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
                payload.protocol_version
            )
            .into());
        }
        return Ok((payload.fields, "json_v2"));
    }

    if let Ok(fields) = serde_json::from_str::<Vec<T>>(mapping) {
        return Ok((fields, "json_legacy_array"));
    }

    Err(format!(
        "Invalid schema mapping payload for {}. Expected JSON object {{\"protocol_version\":...,\"fields\":[...]}}.",
        handoff_name
    )
    .into())
}

fn build_read_scan_plan(
    path: &str,
    variables_as_str: &str,
    mapping: &str,
    safe_relaxed: bool,
    asterisk_var: Option<&str>,
    sql_if: Option<&str>,
    sort: &str,
    random_share: f64,
) -> Result<ReadScanPlan, Box<dyn Error>> {
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
        let (fields, handoff_mode) = parse_schema_handoff_fields::<FieldSpec>(mapping, "read")?;
        set_macro("dtpq_read_schema_handoff", handoff_mode, true);
        fields
    };

    let selected_column_list: Vec<String> = variables_as_str
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let selected_column_names: HashSet<&str> =
        selected_column_list.iter().map(|s| s.as_str()).collect();
    let all_columns: Vec<FieldSpec> = all_columns_unfiltered
        .into_iter()
        .filter(|col_info| selected_column_names.contains(col_info.name.as_str()))
        .collect();

    let transfer_columns = build_transfer_columns(&all_columns);
    let can_use_eager = Path::new(path).is_file()
        && !path.contains('*')
        && !path.contains('?')
        && !safe_relaxed
        && asterisk_var.is_none()
        && sql_if.map(|s| s.trim().is_empty()).unwrap_or(true)
        && sort.trim().is_empty()
        && random_share <= 0.0;

    Ok(ReadScanPlan {
        selected_column_list,
        transfer_columns,
        can_use_eager,
    })
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TransferWriterKind {
    Numeric,
    Date,
    Time,
    Datetime,
    String,
    Strl,
}

#[derive(Clone, Debug)]
struct TransferColumnSpec {
    name: String,
    stata_col_index: usize,
    stata_type: String,
    writer_kind: TransferWriterKind,
}

fn estimated_writer_row_bytes(kind: TransferWriterKind) -> usize {
    match kind {
        TransferWriterKind::Numeric => 8,
        TransferWriterKind::Date => 4,
        TransferWriterKind::Time => 8,
        TransferWriterKind::Datetime => 8,
        TransferWriterKind::String => 48,
        TransferWriterKind::Strl => 128,
    }
}

fn estimate_transfer_row_width_bytes(transfer_columns: &[TransferColumnSpec]) -> usize {
    transfer_columns
        .iter()
        .map(|col| estimated_writer_row_bytes(col.writer_kind))
        .sum::<usize>()
        .max(1)
}

fn publish_read_batch_tuner_metrics(tuner: &AdaptiveBatchTuner) {
    set_macro(
        "dtpq_read_selected_batch_size",
        &tuner.selected_batch_size().to_string(),
        true,
    );
    set_macro(
        "dtpq_read_batch_row_width_bytes",
        &tuner.row_width_bytes().to_string(),
        true,
    );
    set_macro(
        "dtpq_read_batch_memory_cap_rows",
        &tuner.memory_guardrail_rows().to_string(),
        true,
    );
    set_macro(
        "dtpq_read_batch_adjustments",
        &tuner.tuning_adjustments().to_string(),
        true,
    );
    set_macro("dtpq_read_batch_tuner_mode", tuner.tuning_mode(), true);
}

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
    if metadata_lookup_uses_legacy_scan() {
        return has_metadata_key_legacy_scan(path, key);
    }
    crate::metadata::has_parquet_metadata_key(path, key)
}

fn metadata_lookup_uses_legacy_scan() -> bool {
    env::var(ENV_METADATA_LOOKUP_MODE)
        .map(|mode| {
            let mode = mode.trim().to_ascii_lowercase();
            mode == "legacy_scan" || mode == "legacy" || mode == "bytes_scan"
        })
        .unwrap_or(false)
}

fn lazy_execution_uses_legacy_batches() -> bool {
    env::var(ENV_LAZY_EXECUTION_MODE)
        .map(|mode| {
            let mode = mode.trim().to_ascii_lowercase();
            mode == "legacy_batches" || mode == "legacy" || mode == "clone_slice_collect"
        })
        .unwrap_or(false)
}

fn has_metadata_key_legacy_scan(path: &str, key: &str) -> Result<bool, Box<dyn Error>> {
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
    set_macro("dtpq_read_lazy_mode", "eager_fast_path", true);
    set_macro("dtpq_read_selected_batch_size", "0", true);
    set_macro("dtpq_read_batch_row_width_bytes", "0", true);
    set_macro("dtpq_read_batch_memory_cap_rows", "0", true);
    set_macro("dtpq_read_batch_adjustments", "0", true);
    set_macro("dtpq_read_batch_tuner_mode", "fixed", true);
    set_macro("dtpq_if_filter_mode", "none", true);
    set_macro("dtpq_read_schema_handoff", "legacy_macros", true);
    set_macro("dtpq_read_engine_stage", "scan_plan", true);

    let plan = build_read_scan_plan(
        path,
        variables_as_str,
        mapping,
        safe_relaxed,
        asterisk_var,
        sql_if,
        sort,
        random_share,
    )?;
    let selected_column_list: Vec<&str> = plan
        .selected_column_list
        .iter()
        .map(|s| s.as_str())
        .collect();
    let transfer_columns = plan.transfer_columns;

    set_macro("dtpq_read_engine_stage", "execute", true);

    if plan.can_use_eager {
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

        let row_width_bytes = estimate_transfer_row_width_bytes(&transfer_columns);
        let mut batch_tuner = AdaptiveBatchTuner::new(row_width_bytes, batch_size, 0);

        let n_threads = get_compute_thread_pool().current_num_threads().max(1);
        let strategy = parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(selected_column_list.len(), df.height(), n_threads)
        });
        let total_rows = df.height();
        let mut loaded_rows = 0usize;
        let mut batch_offset = 0usize;
        let mut n_batches = 0usize;

        while batch_offset < total_rows {
            let batch_length = (total_rows - batch_offset).min(batch_tuner.selected_batch_size());
            let batch_df = df.slice(batch_offset as i64, batch_length);
            if batch_df.height() == 0 {
                break;
            }
            let batch_started_at = Instant::now();
            set_macro("dtpq_read_engine_stage", "stata_sink", true);
            process_batch_with_strategy(
                &batch_df,
                batch_offset,
                &transfer_columns,
                strategy,
                stata_offset,
            )?;
            let batch_rows = batch_df.height();
            loaded_rows += batch_rows;
            processed_batches += 1;
            n_batches += 1;
            batch_offset += batch_rows;
            batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
        }

        set_macro("n_batches", &n_batches.to_string(), false);
        set_macro("n_loaded_rows", &loaded_rows.to_string(), false);
        publish_read_batch_tuner_metrics(&batch_tuner);
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
        if let Some(expr) = compile_if_expr(sql) {
            lf = lf.filter(expr);
            set_macro("dtpq_if_filter_mode", "expr", true);
        } else {
            let mut ctx = SQLContext::new();
            ctx.register("df", lf);
            let translated = convert_if_sql(sql);
            lf = ctx.execute(&format!("select * from df where {}", translated))?;
            set_macro("dtpq_if_filter_mode", "sql_fallback", true);
        }
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
    let n_threads = get_compute_thread_pool().current_num_threads().max(1);
    let row_width_bytes = estimate_transfer_row_width_bytes(&transfer_columns);
    let mut batch_tuner = AdaptiveBatchTuner::new(row_width_bytes, batch_size, 0);

    let mut loaded_rows = 0usize;
    let n_batches;

    if lazy_execution_uses_legacy_batches() {
        set_macro("dtpq_read_lazy_mode", "legacy_batches", true);

        let strategy = parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(columns.len(), n_rows, n_threads)
        });

        let mut n_batches_legacy = 0usize;
        let mut requested_offset = 0usize;
        while requested_offset < n_rows {
            let mut lf_batch = lf.clone().select(&columns);
            let batch_offset = batch_source_offset + requested_offset;
            let batch_length = (n_rows - requested_offset).min(batch_tuner.selected_batch_size());
            lf_batch = lf_batch.slice(batch_offset as i64, batch_length as u32);
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
            let batch_started_at = Instant::now();
            set_macro("dtpq_read_engine_stage", "stata_sink", true);
            process_batch_with_strategy(
                &batch_df,
                batch_offset - batch_source_offset,
                &transfer_columns,
                strategy,
                stata_offset,
            )?;
            let batch_rows = batch_df.height();
            loaded_rows += batch_rows;
            processed_batches += 1;
            n_batches_legacy += 1;
            requested_offset += batch_length;
            batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
        }

        n_batches = n_batches_legacy;
    } else {
        set_macro("dtpq_read_lazy_mode", "single_pass", true);

        let mut lf_single_pass = lf.select(&columns);
        if batch_source_offset > 0 {
            lf_single_pass = lf_single_pass.slice(batch_source_offset as i64, n_rows as u32);
        }

        let single_pass_df = if use_streaming {
            collect_calls += 1;
            lf_single_pass.with_new_streaming(true).collect()?
        } else {
            collect_calls += 1;
            lf_single_pass.collect()?
        };

        let total_rows = single_pass_df.height();
        let strategy = parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(columns.len(), total_rows, n_threads)
        });

        let mut n_batches_single = 0usize;
        let mut batch_offset = 0usize;
        while batch_offset < total_rows {
            let batch_length = (total_rows - batch_offset).min(batch_tuner.selected_batch_size());
            let batch_df = single_pass_df.slice(batch_offset as i64, batch_length);
            if batch_df.height() == 0 {
                break;
            }
            let batch_started_at = Instant::now();
            set_macro("dtpq_read_engine_stage", "stata_sink", true);
            process_batch_with_strategy(
                &batch_df,
                batch_offset,
                &transfer_columns,
                strategy,
                stata_offset,
            )?;
            let batch_rows = batch_df.height();
            loaded_rows += batch_rows;
            processed_batches += 1;
            n_batches_single += 1;
            batch_offset += batch_rows;
            batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
        }

        n_batches = n_batches_single;
    }

    set_macro("n_batches", &n_batches.to_string(), false);
    set_macro("n_loaded_rows", &loaded_rows.to_string(), false);
    publish_read_batch_tuner_metrics(&batch_tuner);
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

    let mut payload_fields = Vec::with_capacity(schema.len());
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

        payload_fields.push(DescribeFieldPayload {
            name: name.to_string(),
            stata_type: stata_type.to_string(),
            polars_type: polars_type.to_string(),
            string_length,
            rename: String::new(),
        });
    }

    let payload = DescribeSchemaPayload {
        protocol_version: crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
        fields: payload_fields,
    };
    let payload_json = serde_json::to_string(&payload).map_err(|e| {
        PolarsError::ComputeError(format!("failed to encode schema payload: {e}").into())
    })?;
    set_macro(
        "dtpq_schema_protocol_version",
        &crate::SCHEMA_HANDOFF_PROTOCOL_VERSION.to_string(),
        false,
    );
    set_macro("dtpq_schema_payload", &payload_json, false);

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

fn writer_kind_from_stata_type(stata_type: &str) -> TransferWriterKind {
    match stata_type {
        "string" => TransferWriterKind::String,
        "strl" => TransferWriterKind::Strl,
        "date" => TransferWriterKind::Date,
        "time" => TransferWriterKind::Time,
        "datetime" => TransferWriterKind::Datetime,
        _ => TransferWriterKind::Numeric,
    }
}

fn build_transfer_columns(all_columns: &[FieldSpec]) -> Vec<TransferColumnSpec> {
    all_columns
        .iter()
        .map(|col| TransferColumnSpec {
            name: col.name.clone(),
            stata_col_index: col.index,
            stata_type: col.stata_type.clone(),
            writer_kind: writer_kind_from_stata_type(&col.stata_type),
        })
        .collect()
}

#[derive(Copy, Clone, Debug)]
enum CellConversion<T> {
    Value(Option<T>),
    Mismatch,
}

fn convert_boolean_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Boolean(v) => CellConversion::Value(Some(if v { 1.0 } else { 0.0 })),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_i8_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int8(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_i16_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int16(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_i32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int32(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_i64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int64(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_u8_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt8(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_u16_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt16(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_u32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt32(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_u64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt64(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_f32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Float32(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_f64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Float64(v) => CellConversion::Value(Some(v)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_date_to_stata_days(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Date(v) => CellConversion::Value(Some((v + STATA_DATE_ORIGIN) as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_time_to_stata_millis(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Time(v) => CellConversion::Value(Some((v / TIME_US) as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn datetime_unit_factor(unit: TimeUnit) -> f64 {
    match unit {
        TimeUnit::Nanoseconds => (TIME_NS / TIME_MS) as f64,
        TimeUnit::Microseconds => (TIME_US / TIME_MS) as f64,
        TimeUnit::Milliseconds => 1.0,
    }
}

fn datetime_to_stata_clock(value: i64, unit: TimeUnit) -> f64 {
    let sec_shift_scaled = (STATA_EPOCH_MS as f64) * (TIME_MS as f64);
    value as f64 / datetime_unit_factor(unit) + sec_shift_scaled
}

fn convert_datetime_to_stata_clock(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Datetime(v, unit, _) => {
            CellConversion::Value(Some(datetime_to_stata_clock(v, unit)))
        }
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_strict_string(value: AnyValue<'_>) -> CellConversion<String> {
    match value {
        AnyValue::String(v) => CellConversion::Value(Some(v.to_string())),
        AnyValue::StringOwned(v) => CellConversion::Value(Some(v.to_string())),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn convert_numeric_legacy(value: AnyValue<'_>) -> Option<f64> {
    match value {
        AnyValue::Boolean(v) => Some(if v { 1.0 } else { 0.0 }),
        AnyValue::Int8(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Float64(v) => Some(v),
        AnyValue::Date(v) => Some((v + STATA_DATE_ORIGIN) as f64),
        AnyValue::Time(v) => Some((v / TIME_US) as f64),
        _ => None,
    }
}

fn convert_string_legacy(value: AnyValue<'_>) -> Option<String> {
    match value {
        AnyValue::String(v) => Some(v.to_string()),
        AnyValue::StringOwned(v) => Some(v.to_string()),
        AnyValue::Null => None,
        v => Some(v.to_string()),
    }
}

fn convert_datetime_legacy(value: AnyValue<'_>, dtype: &DataType) -> Option<f64> {
    let unit_factor = match dtype {
        DataType::Datetime(TimeUnit::Nanoseconds, _) => (TIME_NS / TIME_MS) as f64,
        DataType::Datetime(TimeUnit::Microseconds, _) => (TIME_US / TIME_MS) as f64,
        DataType::Datetime(TimeUnit::Milliseconds, _) => 1.0,
        _ => 1.0,
    };
    let sec_shift_scaled = (STATA_EPOCH_MS as f64) * (TIME_MS as f64);
    match value {
        AnyValue::Datetime(v, _, _) => Some(v as f64 / unit_factor + sec_shift_scaled),
        _ => None,
    }
}

fn assign_cell_legacy(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    row_idx: usize,
    global_row_idx: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match transfer_column.stata_type.as_str() {
        "string" | "strl" => {
            let out = match col.get(row_idx) {
                Ok(v) => convert_string_legacy(v),
                Err(_) => None,
            };
            replace_string(
                out,
                global_row_idx + 1 + stata_offset,
                transfer_column.stata_col_index + 1,
            );
            Ok(())
        }
        "datetime" => {
            let value = match col.get(row_idx) {
                Ok(v) => convert_datetime_legacy(v, col.dtype()),
                Err(_) => None,
            };
            replace_number(
                value,
                global_row_idx + 1 + stata_offset,
                transfer_column.stata_col_index + 1,
            );
            Ok(())
        }
        _ => {
            let value = match col.get(row_idx) {
                Ok(v) => convert_numeric_legacy(v),
                Err(_) => None,
            };
            replace_number(
                value,
                global_row_idx + 1 + stata_offset,
                transfer_column.stata_col_index + 1,
            );
            Ok(())
        }
    }
}

fn assign_fallback_cell(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    row_idx: usize,
    global_row_idx: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    record_transfer_fallback();
    assign_cell_legacy(col, transfer_column, row_idx, global_row_idx, stata_offset)
}

fn write_numeric_with_converter(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    converter: fn(AnyValue<'_>) -> CellConversion<f64>,
) -> PolarsResult<()> {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        match col.get(row_idx) {
            Ok(value) => match converter(value) {
                CellConversion::Value(number) => {
                    replace_number(
                        number,
                        global_row_idx + 1 + stata_offset,
                        transfer_column.stata_col_index + 1,
                    );
                }
                CellConversion::Mismatch => {
                    record_transfer_conversion_failure();
                    assign_fallback_cell(
                        col,
                        transfer_column,
                        row_idx,
                        global_row_idx,
                        stata_offset,
                    )?;
                }
            },
            Err(_) => {
                record_transfer_conversion_failure();
                assign_fallback_cell(col, transfer_column, row_idx, global_row_idx, stata_offset)?;
            }
        }
    }
    Ok(())
}

fn write_string_with_converter(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    converter: fn(AnyValue<'_>) -> CellConversion<String>,
) -> PolarsResult<()> {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        match col.get(row_idx) {
            Ok(value) => match converter(value) {
                CellConversion::Value(text) => {
                    replace_string(
                        text,
                        global_row_idx + 1 + stata_offset,
                        transfer_column.stata_col_index + 1,
                    );
                }
                CellConversion::Mismatch => {
                    record_transfer_conversion_failure();
                    assign_fallback_cell(
                        col,
                        transfer_column,
                        row_idx,
                        global_row_idx,
                        stata_offset,
                    )?;
                }
            },
            Err(_) => {
                record_transfer_conversion_failure();
                assign_fallback_cell(col, transfer_column, row_idx, global_row_idx, stata_offset)?;
            }
        }
    }
    Ok(())
}

fn write_column_fallback_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        record_transfer_conversion_failure();
        assign_fallback_cell(col, transfer_column, row_idx, global_row_idx, stata_offset)?;
    }
    Ok(())
}

fn write_numeric_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::Boolean => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_boolean_to_f64,
        ),
        DataType::Int8 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i8_to_f64,
        ),
        DataType::Int16 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i16_to_f64,
        ),
        DataType::Int32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i32_to_f64,
        ),
        DataType::Int64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i64_to_f64,
        ),
        DataType::UInt8 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u8_to_f64,
        ),
        DataType::UInt16 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u16_to_f64,
        ),
        DataType::UInt32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u32_to_f64,
        ),
        DataType::UInt64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u64_to_f64,
        ),
        DataType::Float32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_f32_to_f64,
        ),
        DataType::Float64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_f64_to_f64,
        ),
        DataType::Date => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_date_to_stata_days,
        ),
        DataType::Time => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_time_to_stata_millis,
        ),
        DataType::Datetime(_, _) => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_datetime_to_stata_clock,
        ),
        _ => write_column_fallback_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn write_date_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::Date => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_date_to_stata_days,
        ),
        _ => write_column_fallback_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn write_time_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::Time => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_time_to_stata_millis,
        ),
        _ => write_column_fallback_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn write_datetime_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::Datetime(_, _) => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_datetime_to_stata_clock,
        ),
        _ => write_column_fallback_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn write_string_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::String => write_string_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_strict_string,
        ),
        _ => write_column_fallback_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn write_transfer_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match transfer_column.writer_kind {
        TransferWriterKind::Numeric => write_numeric_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
        TransferWriterKind::Date => write_date_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
        TransferWriterKind::Time => write_time_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
        TransferWriterKind::Datetime => write_datetime_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
        TransferWriterKind::String | TransferWriterKind::Strl => write_string_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
) -> PolarsResult<()> {
    let row_count = batch.height();
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        return process_row_range(
            batch,
            start_index,
            0,
            row_count,
            transfer_columns,
            stata_offset,
        );
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
                        transfer_columns,
                        stata_offset,
                    )
                })
        }
        BatchMode::ByColumn => transfer_columns.par_iter().try_for_each(|transfer_column| {
            let col = batch.column(&transfer_column.name)?;
            write_transfer_column_range(
                col,
                transfer_column,
                start_index,
                0,
                row_count,
                stata_offset,
            )
        }),
    })
}

fn process_row_range(
    batch: &DataFrame,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    transfer_columns: &[TransferColumnSpec],
    stata_offset: usize,
) -> PolarsResult<()> {
    for transfer_column in transfer_columns {
        let col = batch.column(&transfer_column.name)?;
        write_transfer_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        )?;
    }
    Ok(())
}
