use polars::prelude::*;
use std::env;
use std::fs::File;
use std::time::Instant;

use crate::config::*;
use crate::downcast::apply_cast;
use crate::error::DtparquetError;
use crate::if_filter::apply_if_filter;
use crate::metrics::*;
use crate::plan::read::*;
use crate::stata_interface::{reset_transfer_metrics, set_macro};
use crate::transfer::*;
use crate::utilities::{get_compute_thread_pool, warm_thread_pools, AdaptiveBatchTuner, BatchMode};

// --- Transformation Logic ---

fn apply_random_sample(
    lf: LazyFrame,
    random_share: f64,
    random_seed: u64,
    collect_calls: &mut usize,
) -> Result<LazyFrame, DtparquetError> {
    if random_share <= 0.0 {
        return Ok(lf);
    }

    let random_seed_option = if random_seed == 0 {
        None
    } else {
        Some(random_seed)
    };
    *collect_calls += 1;
    let sampled = lf.collect()?.sample_frac(
        &Series::new("frac".into(), vec![random_share]),
        false,
        false,
        random_seed_option,
    )?;
    Ok(sampled.lazy())
}

fn apply_sort_transform(mut lf: LazyFrame, sort: &str) -> LazyFrame {
    if sort.is_empty() {
        return lf;
    }

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
    lf
}

// --- Execution Orchestration ---

fn collect_lazy(lf: LazyFrame, use_streaming: bool) -> PolarsResult<DataFrame> {
    if use_streaming {
        lf.collect_with_engine(Engine::Streaming)
    } else {
        lf.collect()
    }
}

pub fn sink_dataframe_in_batches(
    df: &DataFrame,
    start_index_base: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
    batch_tuner: &mut AdaptiveBatchTuner,
    processed_batches: &mut usize,
) -> PolarsResult<(usize, usize)> {
    crate::transfer::sink_dataframe_in_batches(
        df,
        start_index_base,
        transfer_columns,
        strategy,
        stata_offset,
        batch_tuner,
        processed_batches,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_lazy_legacy_batches(
    lf: LazyFrame,
    columns: &[Expr],
    n_rows: usize,
    batch_source_offset: usize,
    use_streaming: bool,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
    batch_tuner: &mut AdaptiveBatchTuner,
    processed_batches: &mut usize,
    collect_calls: &mut usize,
) -> PolarsResult<(usize, usize)> {
    let mut requested_offset = 0usize;
    let mut loaded_rows = 0usize;
    let mut n_batches = 0usize;

    while requested_offset < n_rows {
        let mut lf_batch = lf.clone().select(columns);
        let batch_offset = batch_source_offset + requested_offset;
        let batch_length = (n_rows - requested_offset).min(batch_tuner.selected_batch_size());
        lf_batch = lf_batch.slice(batch_offset as i64, batch_length as u32);
        *collect_calls += 1;
        let batch_df = collect_lazy(lf_batch, use_streaming)?;
        if batch_df.height() == 0 {
            break;
        }

        let _batch_started_at = Instant::now();
        crate::transfer::sink_dataframe_in_batches(
            &batch_df,
            batch_offset - batch_source_offset,
            transfer_columns,
            strategy,
            stata_offset,
            batch_tuner,
            processed_batches,
        )?;

        let batch_rows = batch_df.height();
        loaded_rows += batch_rows;
        n_batches += 1;
        requested_offset += batch_length;
    }

    Ok((loaded_rows, n_batches))
}

#[allow(clippy::too_many_arguments)]
fn run_lazy_single_pass(
    mut lf: LazyFrame,
    columns: &[Expr],
    n_rows: usize,
    batch_source_offset: usize,
    use_streaming: bool,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
    batch_tuner: &mut AdaptiveBatchTuner,
    processed_batches: &mut usize,
    collect_calls: &mut usize,
) -> PolarsResult<(usize, usize)> {
    lf = lf.select(columns);
    if batch_source_offset > 0 {
        lf = lf.slice(batch_source_offset as i64, n_rows as u32);
    }

    *collect_calls += 1;
    let single_pass_df = collect_lazy(lf, use_streaming)?;
    sink_dataframe_in_batches(
        &single_pass_df,
        0,
        transfer_columns,
        strategy,
        stata_offset,
        batch_tuner,
        processed_batches,
    )
}

// --- Main Entry Point ---

#[derive(Copy, Clone)]
pub struct ReadRequest<'a> {
    pub path: &'a str,
    pub variables_as_str: &'a str,
    pub n_rows: usize,
    pub offset: usize,
    pub sql_if: Option<&'a str>,
    pub mapping: &'a str,
    pub parallel_strategy: Option<BatchMode>,
    pub safe_relaxed: bool,
    pub asterisk_var: Option<&'a str>,
    pub order_by: &'a str,
    pub order_by_type: usize,
    pub order_descending: f64,
    pub stata_offset: usize,
    pub random_share: f64,
    pub random_seed: u64,
    pub batch_size: usize,
}

struct ReadEngineOutput {
    n_batches: usize,
    loaded_rows: usize,
    batch_tuner: AdaptiveBatchTuner,
}

fn resolve_inputs_stage(request: ReadRequest<'_>) -> Result<ReadBoundaryInputs, DtparquetError> {
    Ok(resolve_read_boundary_inputs(
        request.variables_as_str,
        request.mapping,
    )?)
}

fn build_plan_stage(
    request: ReadRequest<'_>,
    boundary_inputs: &ReadBoundaryInputs,
) -> Result<ReadScanPlan, DtparquetError> {
    Ok(build_read_scan_plan(
        request.path,
        boundary_inputs,
        request.safe_relaxed,
        request.asterisk_var,
        request.sql_if,
        request.order_by,
        request.random_share,
    )?)
}

fn execute_engine_stage(
    request: ReadRequest<'_>,
    cast_json: &str,
    plan: ReadScanPlan,
    collect_calls: &mut usize,
    processed_batches: &mut usize,
) -> Result<ReadEngineOutput, DtparquetError> {
    let path = request.path;
    let n_rows = request.n_rows;
    let offset = request.offset;
    let sql_if = request.sql_if;
    let parallel_strategy = request.parallel_strategy;
    let safe_relaxed = request.safe_relaxed;
    let asterisk_var = request.asterisk_var;
    let order_by = request.order_by;
    let stata_offset = request.stata_offset;
    let random_share = request.random_share;
    let random_seed = request.random_seed;
    let batch_size = request.batch_size;

    let selected_column_list: Vec<&str> = plan
        .selected_column_list
        .iter()
        .map(|s| s.as_str())
        .collect();
    let transfer_columns = plan.transfer_columns;

    if plan.can_use_eager {
        if !selected_column_list.is_empty() {
            if let Err(e) = crate::schema::validate_parquet_schema(path, &selected_column_list) {
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

        if !cast_json.is_empty() {
            let cast_mapping: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(cast_json)?;
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

        let row_width_bytes = crate::transfer::estimate_transfer_row_width_bytes(&transfer_columns);
        let mut batch_tuner = AdaptiveBatchTuner::new(row_width_bytes, batch_size, 0);

        let n_threads = get_compute_thread_pool().current_num_threads().max(1);
        let strategy = parallel_strategy.unwrap_or_else(|| {
            crate::utilities::determine_parallelization_strategy(
                selected_column_list.len(),
                df.height(),
                n_threads,
            )
        });
        set_read_engine_stage(ReadEngineStage::StataSink);
        let (loaded_rows, n_batches) = sink_dataframe_in_batches(
            &df,
            0,
            &transfer_columns,
            strategy,
            stata_offset,
            &mut batch_tuner,
            processed_batches,
        )?;

        return Ok(ReadEngineOutput {
            n_batches,
            loaded_rows,
            batch_tuner,
        });
    }

    let mut lf = open_parquet_scan(path, safe_relaxed, asterisk_var)?;

    if !cast_json.is_empty() {
        lf = apply_cast(lf, cast_json)?;
    }
    lf = normalize_categorical(&lf)?;

    let has_if_filter = sql_if.map(|s| !s.trim().is_empty()).unwrap_or(false);
    let mut batch_source_offset = offset;
    if has_if_filter {
        lf = lf.slice(offset as i64, n_rows as u32);
        batch_source_offset = 0;
    }
    let (lf_filtered, has_filter_expr) = apply_if_filter(lf, sql_if)?;
    if has_filter_expr {
        set_macro("if_filter_mode", "expr", true);
    }
    let lf_sampled = apply_random_sample(lf_filtered, random_share, random_seed, collect_calls)?;
    lf = apply_sort_transform(lf_sampled, order_by);

    let use_streaming = n_rows > 1_000_000;
    let columns: Vec<Expr> = selected_column_list.iter().map(|s| col(*s)).collect();
    let n_threads = get_compute_thread_pool().current_num_threads().max(1);
    let row_width_bytes = crate::transfer::estimate_transfer_row_width_bytes(&transfer_columns);
    let mut batch_tuner = AdaptiveBatchTuner::new(row_width_bytes, batch_size, 0);

    let mut total_loaded = 0usize;

    let n_batches = if lazy_execution_uses_legacy_batches() {
        set_read_lazy_mode(ReadLazyMode::LegacyBatches);

        let strategy = parallel_strategy.unwrap_or_else(|| {
            crate::utilities::determine_parallelization_strategy(columns.len(), n_rows, n_threads)
        });
        set_read_engine_stage(ReadEngineStage::StataSink);

        let (loaded_rows_legacy, n_batches_legacy) = run_lazy_legacy_batches(
            lf,
            &columns,
            n_rows,
            batch_source_offset,
            use_streaming,
            &transfer_columns,
            strategy,
            stata_offset,
            &mut batch_tuner,
            processed_batches,
            collect_calls,
        )?;
        total_loaded += loaded_rows_legacy;
        n_batches_legacy
    } else {
        set_read_lazy_mode(ReadLazyMode::SinglePass);
        let strategy = parallel_strategy.unwrap_or_else(|| {
            crate::utilities::determine_parallelization_strategy(columns.len(), n_rows, n_threads)
        });
        set_read_engine_stage(ReadEngineStage::StataSink);

        let (loaded_rows_single, n_batches_single) = run_lazy_single_pass(
            lf,
            &columns,
            n_rows,
            batch_source_offset,
            use_streaming,
            &transfer_columns,
            strategy,
            stata_offset,
            &mut batch_tuner,
            processed_batches,
            collect_calls,
        )?;
        total_loaded += loaded_rows_single;
        n_batches_single
    };

    Ok(ReadEngineOutput {
        n_batches,
        loaded_rows: total_loaded,
        batch_tuner,
    })
}

pub fn import_parquet_request(request: &ReadRequest<'_>) -> Result<i32, DtparquetError> {
    let request = *request;

    let started_at = Instant::now();
    let mut collect_calls = 0usize;
    let mut processed_batches = 0usize;
    warm_thread_pools();
    reset_transfer_metrics();
    emit_read_runtime_metrics(&CommonRuntimeMetrics::zero());
    emit_read_init_macros();

    let boundary_inputs = resolve_inputs_stage(request)?;
    let plan = build_plan_stage(request, &boundary_inputs)?;
    emit_read_plan_macros(plan.schema_handoff_mode);

    let engine_output = execute_engine_stage(
        request,
        boundary_inputs.cast_json.as_str(),
        plan,
        &mut collect_calls,
        &mut processed_batches,
    )?;

    finalize_read_runtime(
        engine_output.n_batches,
        engine_output.loaded_rows,
        collect_calls,
        processed_batches,
        &engine_output.batch_tuner,
        started_at,
    );

    Ok(0)
}

// --- Internal State Management ---

#[derive(Copy, Clone)]
enum ReadLazyMode {
    LegacyBatches,
    SinglePass,
}

impl ReadLazyMode {
    fn as_macro_value(&self) -> &'static str {
        match self {
            ReadLazyMode::LegacyBatches => "legacy_batches",
            ReadLazyMode::SinglePass => "single_pass",
        }
    }
}

enum ReadEngineStage {
    ScanPlan,
    Execute,
    StataSink,
}

impl ReadEngineStage {
    fn as_macro_value(&self) -> &'static str {
        match self {
            ReadEngineStage::ScanPlan => "scan_plan",
            ReadEngineStage::Execute => "execute",
            ReadEngineStage::StataSink => "stata_sink",
        }
    }
}

fn set_read_engine_stage(stage: ReadEngineStage) {
    set_macro("read_engine_stage", stage.as_macro_value(), true);
}

fn set_read_lazy_mode(mode: ReadLazyMode) {
    set_macro("read_lazy_mode", mode.as_macro_value(), true);
}

fn emit_read_init_macros() {
    set_macro("read_selected_batch_size", "0", true);
    set_macro("read_batch_row_width_bytes", "0", true);
    set_macro("read_batch_memory_cap_rows", "0", true);
    set_macro("read_batch_adjustments", "0", true);
    set_macro("read_batch_tuner_mode", "fixed", true);
    set_macro("if_filter_mode", "none", true);
    set_macro("read_schema_handoff", "legacy_macros", true);
    set_macro("read_lazy_mode", "none", true);
    set_read_engine_stage(ReadEngineStage::ScanPlan);
}

fn emit_read_plan_macros(schema_handoff_mode: &str) {
    set_macro("read_schema_handoff", schema_handoff_mode, true);
    set_read_engine_stage(ReadEngineStage::Execute);
}

// --- Metrics Wrappers ---

fn emit_read_batch_tuner_metrics(metrics: &CommonBatchTunerMetrics) {
    metrics.emit_to_macros("read");
}

fn emit_read_runtime_metrics(metrics: &CommonRuntimeMetrics) {
    metrics.emit_to_macros("read");
}

fn finalize_read_runtime(
    n_batches: usize,
    loaded_rows: usize,
    collect_calls: usize,
    processed_batches: usize,
    batch_tuner: &AdaptiveBatchTuner,
    started_at: Instant,
) {
    set_read_engine_stage(ReadEngineStage::StataSink);

    let batch_metrics = CommonBatchTunerMetrics::from_tuner(batch_tuner);
    emit_read_batch_tuner_metrics(&batch_metrics);

    let mut metrics = CommonRuntimeMetrics::zero();
    metrics.collect_calls = collect_calls;
    metrics.planned_batches = n_batches;
    metrics.processed_batches = processed_batches;
    metrics.collect(started_at);
    emit_read_runtime_metrics(&metrics);

    set_macro("n_batches", &n_batches.to_string(), false);
    set_macro("loaded_rows", &loaded_rows.to_string(), false);
    set_macro("n_loaded_rows", &loaded_rows.to_string(), false);
}

fn lazy_execution_uses_legacy_batches() -> bool {
    env::var(ENV_LAZY_EXECUTION_MODE)
        .map(|mode| {
            let mode = mode.trim().to_ascii_lowercase();
            mode == "legacy_batches" || mode == "legacy" || mode == "clone_slice_collect"
        })
        .unwrap_or(false)
}
