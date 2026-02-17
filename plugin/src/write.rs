use polars::prelude::*;
use std::fs::create_dir_all;
use std::io::ErrorKind;
use std::path::Path;
use std::time::Instant;

use crate::error::DtparquetError;
use crate::if_filter::compile_if_expr;
use crate::metrics::*;
use crate::plan::write::*;
use crate::stata_interface::{display, reset_transfer_metrics, set_macro};
use crate::transfer::*;
use crate::utilities::{warm_thread_pools, write_pipeline_mode, WritePipelineMode};

// --- Execution Orchestration ---

#[derive(Copy, Clone)]
enum WriteFilterMode {
    None,
    Expr,
}

enum WriteEngineStage {
    ScanPlan,
    Execute,
    StataSink,
}

impl WriteEngineStage {
    fn as_macro_value(&self) -> &'static str {
        match self {
            WriteEngineStage::ScanPlan => "scan_plan",
            WriteEngineStage::Execute => "execute",
            WriteEngineStage::StataSink => "stata_sink",
        }
    }
}

fn set_write_engine_stage(stage: WriteEngineStage) {
    set_macro("write_engine_stage", stage.as_macro_value(), true);
}

fn set_if_filter_mode(mode: WriteFilterMode) {
    match mode {
        WriteFilterMode::None => {}
        WriteFilterMode::Expr => {
            set_macro("if_filter_mode", "expr", true);
        }
    }
}

fn compile_write_filter(
    sql_if: Option<&str>,
) -> Result<(Option<Expr>, WriteFilterMode), DtparquetError> {
    match sql_if.filter(|s| !s.trim().is_empty()) {
        Some(raw) => Ok((Some(compile_if_expr(raw)?), WriteFilterMode::Expr)),
        None => Ok((None, WriteFilterMode::None)),
    }
}

fn build_write_source(
    plan: &WriteScanPlan,
    batch_size: usize,
) -> Result<(Arc<StataRowSource>, LazyFrame), DtparquetError> {
    let scan = Arc::new(StataRowSource::new(
        plan.selected_infos.clone(),
        plan.start_row,
        plan.rows_to_read,
        batch_size,
        plan.row_width_bytes,
    ));
    let lf = LazyFrame::anonymous_scan(scan.clone(), ScanArgsAnonymous::default())?;
    Ok((scan, lf))
}

fn apply_write_filter(
    lf: LazyFrame,
    sql_if: Option<&str>,
) -> Result<(LazyFrame, WriteFilterMode), DtparquetError> {
    let (filter_expr, filter_mode) = compile_write_filter(sql_if)?;
    let filtered = match filter_expr {
        Some(expr) => lf.filter(expr),
        None => lf,
    };
    Ok((filtered, filter_mode))
}

fn sink_write_plan(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    plan: &WriteScanPlan,
    collect_calls: &mut usize,
) -> Result<(), DtparquetError> {
    if plan.partition_cols.is_empty() {
        write_single_dataframe(
            path,
            lf,
            compression,
            compression_level,
            overwrite_partition,
            &plan.dtmeta_json,
            collect_calls,
        )
    } else {
        *collect_calls += 1;
        let mut df = lf.collect()?;
        write_partitioned_dataframe(
            path,
            &mut df,
            compression,
            compression_level,
            &plan.partition_cols,
            overwrite_partition,
            &plan.dtmeta_json,
        )
    }
}

fn resolve_inputs_stage(
    varlist: &str,
    mapping: &str,
) -> Result<WriteBoundaryInputs, DtparquetError> {
    Ok(crate::plan::write::resolve_write_boundary_inputs(
        varlist, mapping,
    )?)
}

fn build_plan_stage(
    boundary_inputs: &WriteBoundaryInputs,
    n_rows: usize,
    offset: usize,
    partition_by: &str,
) -> Result<WriteScanPlan, DtparquetError> {
    Ok(crate::plan::write::build_write_scan_plan(
        boundary_inputs,
        n_rows,
        offset,
        partition_by,
    )?)
}

fn execute_engine_stage(
    request: WriteRequest<'_>,
    plan: &WriteScanPlan,
    collect_calls: &mut usize,
) -> Result<Arc<StataRowSource>, DtparquetError> {
    let (scan, lf) = build_write_source(plan, request.batch_size)?;
    let (lf, filter_mode) = apply_write_filter(lf, request.sql_if)?;
    set_if_filter_mode(filter_mode);
    sink_write_plan(
        request.path,
        lf,
        request.compression,
        request.compression_level,
        request.overwrite_partition,
        plan,
        collect_calls,
    )?;
    Ok(scan)
}

// --- Main Entry Point ---

#[derive(Copy, Clone)]
pub struct WriteRequest<'a> {
    pub path: &'a str,
    pub varlist: &'a str,
    pub n_rows: usize,
    pub offset: usize,
    pub sql_if: Option<&'a str>,
    pub mapping: &'a str,
    pub parallel_strategy: Option<crate::utilities::BatchMode>,
    pub partition_by: &'a str,
    pub compression: &'a str,
    pub compression_level: Option<usize>,
    pub overwrite_partition: bool,
    pub compress: bool,
    pub compress_string: bool,
    pub batch_size: usize,
}

pub fn export_parquet_request(request: &WriteRequest<'_>) -> Result<i32, DtparquetError> {
    let request = *request;
    let varlist = request.varlist;
    let n_rows = request.n_rows;
    let offset = request.offset;
    let mapping = request.mapping;
    let partition_by = request.partition_by;
    let _parallel_strategy = request.parallel_strategy;
    let _compress = request.compress;
    let _compress_string = request.compress_string;

    let started_at = Instant::now();
    let mut collect_calls = 0usize;
    init_write_runtime();
    maybe_warn_deprecated_queue_mode();
    let boundary_inputs = resolve_inputs_stage(varlist, mapping)?;
    let plan = build_plan_stage(&boundary_inputs, n_rows, offset, partition_by)?;
    emit_write_plan_macros(plan.schema_handoff_mode);

    let scan = execute_engine_stage(request, &plan, &mut collect_calls)?;

    scan.join_pipeline_worker();
    finalize_write_runtime(&scan, collect_calls, started_at);

    Ok(0)
}

// --- Internal State Management ---

fn emit_write_init_macros() {
    set_macro("write_selected_batch_size", "0", true);
    set_macro("write_batch_row_width_bytes", "0", true);
    set_macro("write_batch_memory_cap_rows", "0", true);
    set_macro("write_batch_adjustments", "0", true);
    set_macro("write_batch_tuner_mode", "fixed", true);
    set_macro("if_filter_mode", "none", true);
    set_macro("write_schema_handoff", "legacy_macros", true);
    set_write_engine_stage(WriteEngineStage::ScanPlan);
}

fn emit_write_plan_macros(schema_handoff_mode: &str) {
    set_macro("write_schema_handoff", schema_handoff_mode, true);
    set_write_engine_stage(WriteEngineStage::Execute);
}

fn init_write_runtime() {
    warm_thread_pools();
    reset_transfer_metrics();
    emit_write_runtime_metrics(&CommonRuntimeMetrics::zero());
    emit_write_init_macros();
    emit_write_queue_metrics(&WriteQueueMetrics::legacy_direct_zero());
}

fn maybe_warn_deprecated_queue_mode() {
    if write_pipeline_mode() == WritePipelineMode::ProducerConsumer {
        emit_write_queue_deprecated_macros();
        display("dtparquet: queue write mode is deprecated; using direct mode");
    }
}

fn emit_write_queue_deprecated_macros() {
    set_macro("write_pipeline_mode", "legacy_direct", true);
    set_macro(
        "write_pipeline_deprecated",
        "queue_mode_forced_direct",
        true,
    );
}

fn finalize_write_runtime(scan: &StataRowSource, collect_calls: usize, started_at: Instant) {
    set_write_engine_stage(WriteEngineStage::StataSink);

    let batch_tuner = scan.batch_tuner_snapshot();
    let batch_metrics = CommonBatchTunerMetrics::from_tuner(&batch_tuner);
    emit_write_batch_tuner_metrics(&batch_metrics);
    let queue_metrics = snapshot_write_queue_metrics(scan);
    emit_write_queue_metrics(&queue_metrics);

    let mut metrics = CommonRuntimeMetrics::zero();
    metrics.collect_calls = collect_calls;
    metrics.planned_batches = scan.planned_batches();
    metrics.processed_batches = scan.processed_batches();
    metrics.collect(started_at);
    emit_write_runtime_metrics(&metrics);
}

// --- Metrics Wrappers ---

fn emit_write_batch_tuner_metrics(metrics: &CommonBatchTunerMetrics) {
    metrics.emit_to_macros("write");
}

fn emit_write_runtime_metrics(metrics: &CommonRuntimeMetrics) {
    metrics.emit_to_macros("write");
}

struct WriteQueueMetrics {
    mode: &'static str,
    queue_capacity: usize,
    queue_peak: usize,
    queue_backpressure_events: usize,
    queue_wait_ms: usize,
    produced_batches: usize,
    consumed_batches: usize,
}

impl WriteQueueMetrics {
    fn legacy_direct_zero() -> Self {
        Self {
            mode: "legacy_direct",
            queue_capacity: 0,
            queue_peak: 0,
            queue_backpressure_events: 0,
            queue_wait_ms: 0,
            produced_batches: 0,
            consumed_batches: 0,
        }
    }
}

fn snapshot_write_queue_metrics(scan: &StataRowSource) -> WriteQueueMetrics {
    WriteQueueMetrics {
        mode: scan.pipeline_mode_name(),
        queue_capacity: scan.queue_capacity(),
        queue_peak: scan.queue_peak(),
        queue_backpressure_events: scan.queue_backpressure_events(),
        queue_wait_ms: scan.queue_wait_ms(),
        produced_batches: scan.queue_produced_batches(),
        consumed_batches: scan.queue_consumed_batches(),
    }
}

fn emit_write_queue_metrics(metrics: &WriteQueueMetrics) {
    set_macro("write_pipeline_mode", metrics.mode, true);
    set_macro(
        "write_queue_capacity",
        &metrics.queue_capacity.to_string(),
        true,
    );
    set_macro("write_queue_peak", &metrics.queue_peak.to_string(), true);
    set_macro(
        "write_queue_bp_events",
        &metrics.queue_backpressure_events.to_string(),
        true,
    );
    set_macro(
        "write_queue_wait_ms",
        &metrics.queue_wait_ms.to_string(),
        true,
    );
    set_macro(
        "write_queue_prod_batches",
        &metrics.produced_batches.to_string(),
        true,
    );
    set_macro(
        "write_queue_cons_batches",
        &metrics.consumed_batches.to_string(),
        true,
    );
}

// --- Parquet Low-level Write ---

fn write_single_dataframe(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    dtmeta_json: &str,
    collect_calls: &mut usize,
) -> Result<(), DtparquetError> {
    let out_path = Path::new(path);

    if out_path.exists() && !overwrite_partition {
        return Err(format!("Output path exists and overwrite is disabled: {}", path).into());
    }

    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent)?;
        }
    }

    if out_path.exists() && overwrite_partition {
        match std::fs::remove_file(out_path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }

    let tmp_path = format!("{}.tmp", path);
    let key_value_metadata = KeyValueMetadata::from_static(vec![(
        crate::metadata::DTMETA_KEY.to_string(),
        dtmeta_json.to_string(),
    )]);

    let write_options = ParquetWriteOptions {
        compression: parquet_compression(compression, compression_level)?,
        key_value_metadata: Some(key_value_metadata),
        ..Default::default()
    };

    let sink_target = SinkTarget::Path(PlPath::new(&tmp_path));
    *collect_calls += 1;
    lf.sink_parquet(sink_target, write_options, None, SinkOptions::default())?
        .collect()?;

    match std::fs::rename(&tmp_path, path) {
        Ok(()) => {}
        Err(_) => {
            if out_path.exists() {
                let _ = std::fs::remove_file(out_path);
            }
            std::fs::copy(&tmp_path, path)?;
            std::fs::remove_file(&tmp_path)?;
        }
    }
    Ok(())
}

fn write_partitioned_dataframe(
    path: &str,
    df: &mut DataFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &[PlSmallStr],
    overwrite_partition: bool,
    dtmeta_json: &str,
) -> Result<(), DtparquetError> {
    let out_path = Path::new(path);

    if out_path.exists() {
        if !overwrite_partition {
            return Err(format!("Output path exists and overwrite is disabled: {}", path).into());
        }

        if out_path.is_file() {
            std::fs::remove_file(out_path)?;
        } else {
            std::fs::remove_dir_all(out_path)?;
        }
    }

    create_dir_all(out_path)?;

    let key_value_metadata = KeyValueMetadata::from_static(vec![(
        crate::metadata::DTMETA_KEY.to_string(),
        dtmeta_json.to_string(),
    )]);
    let write_options = ParquetWriteOptions {
        compression: parquet_compression(compression, compression_level)?,
        key_value_metadata: Some(key_value_metadata),
        ..Default::default()
    };

    write_partitioned_dataset(
        df,
        PlPathRef::Local(out_path),
        partition_by.to_vec(),
        &write_options,
        None,
        100_000,
    )?;

    Ok(())
}

fn parquet_compression(
    compression: &str,
    compression_level: Option<usize>,
) -> Result<ParquetCompression, DtparquetError> {
    if compression_level.is_some() {
        return Err(DtparquetError::Custom(
            "compression levels are not supported; pass -1".to_string(),
        ));
    }

    match compression {
        "lz4" => Ok(ParquetCompression::Lz4Raw),
        "uncompressed" => Ok(ParquetCompression::Uncompressed),
        "snappy" => Ok(ParquetCompression::Snappy),
        "gzip" => Ok(ParquetCompression::Gzip(None)),
        "lzo" => Ok(ParquetCompression::Lzo),
        "brotli" => Ok(ParquetCompression::Brotli(None)),
        _ => Ok(ParquetCompression::Zstd(None)),
    }
}
