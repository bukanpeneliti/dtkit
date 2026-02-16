#![allow(clippy::too_many_arguments)]

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::create_dir_all;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::boundary::{resolve_arg_or_macro, resolve_schema_handoff};
use crate::if_filter::compile_if_expr;
use crate::mapping::{
    estimate_export_field_width_bytes, export_field_polars_dtype, is_stata_date_format,
    is_stata_datetime_format, is_stata_string_dtype,
};
use crate::metadata::{extract_dtmeta, DTMETA_KEY};
use crate::stata_interface::{
    count_rows, display, publish_transfer_metrics, pull_numeric_cell, pull_string_cell_with_buffer,
    pull_strl_cell_with_arena, read_macro, reset_transfer_metrics, set_macro, StrlArena,
};
use crate::utilities::{
    compute_pool_init_count, get_compute_thread_pool, get_io_thread_pool, io_pool_init_count,
    warm_thread_pools, write_pipeline_mode, AdaptiveBatchTuner, WritePipelineMode,
    STATA_DATE_ORIGIN, STATA_EPOCH_MS, TIME_MS,
};

struct WriteRuntimeMetrics {
    collect_calls: usize,
    planned_batches: usize,
    processed_batches: usize,
    elapsed_ms: u128,
    compute_pool_threads: usize,
    compute_pool_inits: usize,
    io_pool_threads: usize,
    io_pool_inits: usize,
}

impl WriteRuntimeMetrics {
    fn zero() -> Self {
        Self {
            collect_calls: 0,
            planned_batches: 0,
            processed_batches: 0,
            elapsed_ms: 0,
            compute_pool_threads: get_compute_thread_pool().current_num_threads(),
            compute_pool_inits: compute_pool_init_count(),
            io_pool_threads: get_io_thread_pool().current_num_threads(),
            io_pool_inits: io_pool_init_count(),
        }
    }
}

fn snapshot_write_runtime_metrics(
    collect_calls: usize,
    planned_batches: usize,
    processed_batches: usize,
    started_at: Instant,
) -> WriteRuntimeMetrics {
    WriteRuntimeMetrics {
        collect_calls,
        planned_batches,
        processed_batches,
        elapsed_ms: started_at.elapsed().as_millis(),
        compute_pool_threads: get_compute_thread_pool().current_num_threads(),
        compute_pool_inits: compute_pool_init_count(),
        io_pool_threads: get_io_thread_pool().current_num_threads(),
        io_pool_inits: io_pool_init_count(),
    }
}

fn emit_write_runtime_metrics(metrics: &WriteRuntimeMetrics) {
    set_macro(
        "dtpq_write_collect_calls",
        &metrics.collect_calls.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_planned_batches",
        &metrics.planned_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_processed_batches",
        &metrics.processed_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_elapsed_ms",
        &metrics.elapsed_ms.to_string(),
        true,
    );
    set_macro(
        "dtpq_compute_pool_threads",
        &metrics.compute_pool_threads.to_string(),
        true,
    );
    set_macro(
        "dtpq_compute_pool_inits",
        &metrics.compute_pool_inits.to_string(),
        true,
    );
    set_macro(
        "dtpq_io_pool_threads",
        &metrics.io_pool_threads.to_string(),
        true,
    );
    set_macro(
        "dtpq_io_pool_inits",
        &metrics.io_pool_inits.to_string(),
        true,
    );
    publish_transfer_metrics("dtpq_write");
}

struct WriteBatchTunerMetrics {
    selected_batch_size: usize,
    row_width_bytes: usize,
    memory_cap_rows: usize,
    adjustments: usize,
    tuner_mode: &'static str,
}

impl WriteBatchTunerMetrics {
    fn from_tuner(tuner: &AdaptiveBatchTuner) -> Self {
        Self {
            selected_batch_size: tuner.selected_batch_size(),
            row_width_bytes: tuner.row_width_bytes(),
            memory_cap_rows: tuner.memory_guardrail_rows(),
            adjustments: tuner.tuning_adjustments(),
            tuner_mode: tuner.tuning_mode(),
        }
    }
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

fn emit_write_batch_tuner_metrics(metrics: &WriteBatchTunerMetrics) {
    set_macro(
        "dtpq_write_selected_batch_size",
        &metrics.selected_batch_size.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_row_width_bytes",
        &metrics.row_width_bytes.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_memory_cap_rows",
        &metrics.memory_cap_rows.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_adjustments",
        &metrics.adjustments.to_string(),
        true,
    );
    set_macro("dtpq_write_batch_tuner_mode", metrics.tuner_mode, true);
}

fn emit_write_queue_metrics(metrics: &WriteQueueMetrics) {
    set_macro("dtpq_write_pipeline_mode", metrics.mode, true);
    set_macro(
        "dtpq_write_queue_capacity",
        &metrics.queue_capacity.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_peak",
        &metrics.queue_peak.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_bp_events",
        &metrics.queue_backpressure_events.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_wait_ms",
        &metrics.queue_wait_ms.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_prod_batches",
        &metrics.produced_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_cons_batches",
        &metrics.consumed_batches.to_string(),
        true,
    );
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExportField {
    #[serde(alias = "n")]
    pub name: String,
    #[serde(alias = "d")]
    pub dtype: String,
    #[serde(alias = "f")]
    pub format: String,
    #[serde(alias = "l")]
    pub str_length: usize,
}

#[derive(Clone, Debug)]
struct WriteScanPlan {
    selected_infos: Vec<ExportField>,
    start_row: usize,
    rows_to_read: usize,
    row_width_bytes: usize,
    partition_cols: Vec<PlSmallStr>,
    dtmeta_json: String,
    schema_handoff_mode: &'static str,
}

struct WriteBoundaryInputs {
    selected_vars: String,
    all_columns: Vec<ExportField>,
    schema_handoff_mode: &'static str,
}

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
    set_macro("dtpq_write_engine_stage", stage.as_macro_value(), true);
}

fn set_if_filter_mode(mode: WriteFilterMode) {
    match mode {
        WriteFilterMode::None => {}
        WriteFilterMode::Expr => {
            set_macro("dtpq_if_filter_mode", "expr", true);
        }
    }
}

fn emit_write_init_macros() {
    set_macro("dtpq_write_selected_batch_size", "0", true);
    set_macro("dtpq_write_batch_row_width_bytes", "0", true);
    set_macro("dtpq_write_batch_memory_cap_rows", "0", true);
    set_macro("dtpq_write_batch_adjustments", "0", true);
    set_macro("dtpq_write_batch_tuner_mode", "fixed", true);
    set_macro("dtpq_if_filter_mode", "none", true);
    set_macro("dtpq_write_schema_handoff", "legacy_macros", true);
    set_write_engine_stage(WriteEngineStage::ScanPlan);
}

fn emit_write_plan_macros(schema_handoff_mode: &str) {
    set_macro("dtpq_write_schema_handoff", schema_handoff_mode, true);
    set_write_engine_stage(WriteEngineStage::Execute);
}

fn compile_write_filter(
    sql_if: Option<&str>,
) -> Result<(Option<Expr>, WriteFilterMode), Box<dyn Error>> {
    match sql_if.filter(|s| !s.trim().is_empty()) {
        Some(raw) => Ok((Some(compile_if_expr(raw)?), WriteFilterMode::Expr)),
        None => Ok((None, WriteFilterMode::None)),
    }
}

fn build_write_source(
    plan: &WriteScanPlan,
    batch_size: usize,
) -> Result<(Arc<StataRowSource>, LazyFrame), Box<dyn Error>> {
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
) -> Result<(LazyFrame, WriteFilterMode), Box<dyn Error>> {
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
) -> Result<(), Box<dyn Error>> {
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

fn init_write_runtime() {
    warm_thread_pools();
    reset_transfer_metrics();
    emit_write_runtime_metrics(&WriteRuntimeMetrics::zero());
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
    set_macro("dtpq_write_pipeline_mode", "legacy_direct", true);
    set_macro(
        "dtpq_write_pipeline_deprecated",
        "queue_mode_forced_direct",
        true,
    );
}

fn finalize_write_runtime(scan: &StataRowSource, collect_calls: usize, started_at: Instant) {
    set_write_engine_stage(WriteEngineStage::StataSink);

    let batch_tuner = scan.batch_tuner_snapshot();
    let batch_metrics = WriteBatchTunerMetrics::from_tuner(&batch_tuner);
    emit_write_batch_tuner_metrics(&batch_metrics);
    let queue_metrics = snapshot_write_queue_metrics(scan);
    emit_write_queue_metrics(&queue_metrics);

    let metrics = snapshot_write_runtime_metrics(
        collect_calls,
        scan.planned_batches(),
        scan.processed_batches(),
        started_at,
    );
    emit_write_runtime_metrics(&metrics);
}

fn resolve_write_boundary_inputs(
    varlist: &str,
    mapping: &str,
) -> Result<WriteBoundaryInputs, Box<dyn Error>> {
    let selected_vars =
        resolve_arg_or_macro(varlist, "from_macro", "varlist", Some(10 * 1024 * 1024));

    let (all_columns, schema_handoff_mode): (Vec<ExportField>, &'static str) =
        resolve_schema_handoff(
            mapping,
            "save",
            crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
            || {
                let var_count = read_macro("var_count", false, None).parse::<usize>()?;
                column_info_from_macros(var_count)
            },
        )?;

    Ok(WriteBoundaryInputs {
        selected_vars,
        all_columns,
        schema_handoff_mode,
    })
}

fn build_write_scan_plan(
    boundary_inputs: &WriteBoundaryInputs,
    n_rows: usize,
    offset: usize,
    partition_by: &str,
) -> Result<WriteScanPlan, Box<dyn Error>> {
    let selected_vars = boundary_inputs.selected_vars.as_str();
    let all_columns = boundary_inputs.all_columns.clone();

    validate_stata_schema(&all_columns)?;

    let info_by_name: HashMap<&str, &ExportField> = all_columns
        .iter()
        .map(|info| (info.name.as_str(), info))
        .collect();
    let selected_names: Vec<&str> = selected_vars.split_whitespace().collect();
    let selected_infos: Vec<ExportField> = if selected_names.is_empty() {
        all_columns.clone()
    } else {
        selected_names
            .iter()
            .map(|name| {
                *info_by_name
                    .get(*name)
                    .unwrap_or_else(|| panic!("Missing macro metadata for variable {}", name))
            })
            .cloned()
            .collect()
    };

    let total_rows = count_rows() as usize;
    let start_row = offset.min(total_rows);
    let rows_available = total_rows - start_row;
    let rows_to_read = if n_rows == 0 {
        rows_available
    } else {
        n_rows.min(rows_available)
    };

    let row_width_bytes = estimate_export_row_width_bytes(&selected_infos);
    let partition_cols: Vec<PlSmallStr> = partition_by
        .split_whitespace()
        .map(PlSmallStr::from)
        .collect();
    let dtmeta_json = extract_dtmeta();

    Ok(WriteScanPlan {
        selected_infos,
        start_row,
        rows_to_read,
        row_width_bytes,
        partition_cols,
        dtmeta_json,
        schema_handoff_mode: boundary_inputs.schema_handoff_mode,
    })
}

fn estimate_export_row_width_bytes(infos: &[ExportField]) -> usize {
    infos
        .iter()
        .map(|info| estimate_export_field_width_bytes(&info.dtype, info.str_length))
        .sum::<usize>()
        .max(1)
}

#[derive(Default)]
struct WriteQueueTelemetry {
    queue_peak: AtomicUsize,
    backpressure_events: AtomicUsize,
    queue_wait_ms: AtomicUsize,
}

pub struct StataRowSource {
    column_info: Vec<ExportField>,
    start_row: usize,
    n_rows: usize,
    batch_size_hint: Arc<AtomicUsize>,
    planned_batches: usize,
    current_offset: AtomicUsize,
    processed_batches: AtomicUsize,
    schema: Arc<Schema>,
    batch_tuner: Arc<Mutex<AdaptiveBatchTuner>>,
    queue_capacity: usize,
    queue_telemetry: Arc<WriteQueueTelemetry>,
}

impl StataRowSource {
    pub fn new(
        column_info: Vec<ExportField>,
        start_row: usize,
        n_rows: usize,
        configured_batch_size: usize,
        row_width_bytes: usize,
    ) -> Self {
        let mut fields = Vec::with_capacity(column_info.len());
        for info in &column_info {
            let dtype = export_field_polars_dtype(&info.dtype, &info.format);
            fields.push(Field::new(PlSmallStr::from(&info.name), dtype));
        }

        let batch_tuner = Arc::new(Mutex::new(AdaptiveBatchTuner::new(
            row_width_bytes,
            configured_batch_size,
            0,
        )));
        let safe_batch_size = batch_tuner.lock().unwrap().selected_batch_size().max(1);
        let planned_batches = if n_rows == 0 {
            0
        } else {
            n_rows.div_ceil(safe_batch_size)
        };

        let _requested_pipeline_mode = write_pipeline_mode();
        let queue_capacity = 0;

        let batch_size_hint = Arc::new(AtomicUsize::new(safe_batch_size));
        let queue_telemetry = Arc::new(WriteQueueTelemetry::default());

        StataRowSource {
            column_info,
            start_row,
            n_rows,
            batch_size_hint,
            planned_batches,
            current_offset: AtomicUsize::new(0),
            processed_batches: AtomicUsize::new(0),
            schema: Arc::new(Schema::from_iter(fields)),
            batch_tuner,
            queue_capacity,
            queue_telemetry,
        }
    }

    pub fn planned_batches(&self) -> usize {
        let processed = self.processed_batches.load(Ordering::Relaxed);
        if processed == 0 {
            self.planned_batches
        } else {
            processed
        }
    }

    pub fn processed_batches(&self) -> usize {
        self.processed_batches.load(Ordering::Relaxed)
    }

    pub fn batch_tuner_snapshot(&self) -> AdaptiveBatchTuner {
        self.batch_tuner.lock().unwrap().clone()
    }

    pub fn pipeline_mode_name(&self) -> &'static str {
        "legacy_direct"
    }

    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    pub fn queue_peak(&self) -> usize {
        self.queue_telemetry.queue_peak.load(Ordering::Relaxed)
    }

    pub fn queue_backpressure_events(&self) -> usize {
        self.queue_telemetry
            .backpressure_events
            .load(Ordering::Relaxed)
    }

    pub fn queue_wait_ms(&self) -> usize {
        self.queue_telemetry.queue_wait_ms.load(Ordering::Relaxed)
    }

    pub fn queue_produced_batches(&self) -> usize {
        self.processed_batches()
    }

    pub fn queue_consumed_batches(&self) -> usize {
        self.processed_batches()
    }

    fn join_pipeline_worker(&self) {
        let _ = self;
    }

    fn next_batch_legacy(&self) -> PolarsResult<Option<DataFrame>> {
        let requested_batch_size = self.batch_size_hint.load(Ordering::Relaxed).max(1);
        let offset = self
            .current_offset
            .fetch_add(requested_batch_size, Ordering::Relaxed);
        if offset >= self.n_rows {
            return Ok(None);
        }
        let read_count = std::cmp::min(requested_batch_size, self.n_rows - offset);

        let batch_started_at = Instant::now();
        let df = self.read_batch(offset, read_count)?;
        let batch_rows = df.height();
        if batch_rows > 0 {
            self.processed_batches.fetch_add(1, Ordering::Relaxed);
            let next_batch_size = {
                let mut tuner = self.batch_tuner.lock().unwrap();
                tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis())
            };
            self.batch_size_hint
                .store(next_batch_size.max(1), Ordering::Relaxed);
        }
        Ok(Some(df))
    }

    fn scan_legacy(&self) -> PolarsResult<DataFrame> {
        let offset = self
            .current_offset
            .fetch_add(self.n_rows, Ordering::Relaxed);
        if offset >= self.n_rows {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }
        let read_count = std::cmp::min(self.n_rows - offset, self.n_rows);

        let batch_started_at = Instant::now();
        let df = self.read_batch(offset, read_count)?;
        let batch_rows = df.height();
        if batch_rows > 0 {
            self.processed_batches.fetch_add(1, Ordering::Relaxed);
            let next_batch_size = {
                let mut tuner = self.batch_tuner.lock().unwrap();
                tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis())
            };
            self.batch_size_hint
                .store(next_batch_size.max(1), Ordering::Relaxed);
        }
        Ok(df)
    }
}

impl AnonymousScan for StataRowSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<Arc<Schema>> {
        Ok(self.schema.clone())
    }

    fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        self.scan_legacy()
    }

    fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        self.next_batch_legacy()
    }
}

impl StataRowSource {
    fn read_batch(&self, batch_offset: usize, batch_rows: usize) -> PolarsResult<DataFrame> {
        read_batch_from_columns(&self.column_info, self.start_row + batch_offset, batch_rows)
    }
}

fn read_batch_from_columns(
    column_info: &[ExportField],
    offset: usize,
    n_rows: usize,
) -> PolarsResult<DataFrame> {
    let mut columns = Vec::with_capacity(column_info.len());
    for (idx, info) in column_info.iter().enumerate() {
        columns.push(series_from_stata_column(idx + 1, info, offset, n_rows)?);
    }
    Ok(DataFrame::from_iter(columns))
}

fn validate_stata_schema(infos: &[ExportField]) -> Result<(), Box<dyn Error>> {
    let total_rows = count_rows();
    if total_rows == 0 {
        return Err("No rows in Stata data to export".into());
    }

    for info in infos {
        let col_idx = info.name.parse::<usize>().unwrap_or(0);
        if col_idx == 0 {
            continue;
        }

        if is_stata_string_dtype(&info.dtype) {
            continue;
        }

        if let Some(val) = pull_numeric_cell(col_idx, 1) {
            if val.is_nan() && info.dtype != "float" && info.dtype != "double" {
                return Err(format!(
                    "Column '{}' has NaN values but Stata type '{}' cannot store them",
                    info.name, info.dtype
                )
                .into());
            }
        }
    }

    Ok(())
}

pub fn export_parquet(
    path: &str,
    varlist: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    _parallel_strategy: Option<crate::utilities::BatchMode>,
    partition_by: &str,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    _compress: bool,
    _compress_string: bool,
    batch_size: usize,
) -> Result<i32, Box<dyn Error>> {
    let started_at = Instant::now();
    let mut collect_calls = 0usize;
    init_write_runtime();
    maybe_warn_deprecated_queue_mode();
    let boundary_inputs = resolve_write_boundary_inputs(varlist, mapping)?;

    let plan = build_write_scan_plan(&boundary_inputs, n_rows, offset, partition_by)?;
    emit_write_plan_macros(plan.schema_handoff_mode);

    let (scan, lf) = build_write_source(&plan, batch_size)?;
    let (lf, filter_mode) = apply_write_filter(lf, sql_if)?;
    set_if_filter_mode(filter_mode);
    sink_write_plan(
        path,
        lf,
        compression,
        compression_level,
        overwrite_partition,
        &plan,
        &mut collect_calls,
    )?;

    scan.join_pipeline_worker();
    finalize_write_runtime(&scan, collect_calls, started_at);

    Ok(0)
}

fn write_single_dataframe(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    dtmeta_json: &str,
    collect_calls: &mut usize,
) -> Result<(), Box<dyn Error>> {
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
            Err(e) => return Err(Box::new(e)),
        }
    }

    let tmp_path = format!("{}.tmp", path);
    let key_value_metadata =
        KeyValueMetadata::from_static(vec![(DTMETA_KEY.to_string(), dtmeta_json.to_string())]);

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
) -> Result<(), Box<dyn Error>> {
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

    let key_value_metadata =
        KeyValueMetadata::from_static(vec![(DTMETA_KEY.to_string(), dtmeta_json.to_string())]);
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
) -> Result<ParquetCompression, Box<dyn Error>> {
    if compression_level.is_some() {
        return Err("compression levels are not supported; pass -1".into());
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

fn column_info_from_macros(n_vars: usize) -> Result<Vec<ExportField>, Box<dyn Error>> {
    (1..=n_vars)
        .map(|i| {
            let str_length_raw = read_macro(&format!("str_length_{}", i), false, None);
            let str_length = str_length_raw.parse::<usize>().map_err(|_| {
                format!(
                    "Invalid macro str_length_{}='{}': expected usize",
                    i, str_length_raw
                )
            })?;
            Ok(ExportField {
                name: read_macro(&format!("name_{}", i), false, None),
                dtype: read_macro(&format!("dtype_{}", i), false, None).to_lowercase(),
                format: read_macro(&format!("format_{}", i), false, None).to_lowercase(),
                str_length,
            })
        })
        .collect()
}

fn series_from_stata_column(
    stata_col_index: usize,
    info: &ExportField,
    offset: usize,
    n_rows: usize,
) -> Result<Series, PolarsError> {
    if info.dtype == "strl" {
        let mut strl_arena = StrlArena::new();
        let values: Vec<Option<String>> = (0..n_rows)
            .map(|row_idx| {
                pull_strl_cell_with_arena(stata_col_index, offset + row_idx + 1, &mut strl_arena)
                    .ok()
            })
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if is_stata_string_dtype(&info.dtype) {
        let width = info.str_length.max(1);
        let mut str_buffer: Vec<i8> = vec![0; width.saturating_add(1)];
        let values: Vec<String> = (0..n_rows)
            .map(|row_idx| {
                pull_string_cell_with_buffer(stata_col_index, offset + row_idx + 1, &mut str_buffer)
            })
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if is_stata_date_format(&info.format) {
        let values: Vec<Option<i32>> = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i32 - STATA_DATE_ORIGIN)
            })
            .collect();
        return Series::new((&info.name).into(), values).cast(&DataType::Date);
    }

    if is_stata_datetime_format(&info.format) {
        let values: Vec<Option<i64>> = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i64 - ((STATA_EPOCH_MS as f64) * (TIME_MS as f64)) as i64)
            })
            .collect();
        return Series::new((&info.name).into(), values)
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None));
    }

    match info.dtype.as_str() {
        "byte" => {
            let values: Vec<Option<i8>> = (0..n_rows)
                .map(|row_idx| {
                    pull_numeric_cell(stata_col_index, offset + row_idx + 1).map(|v| v as i8)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "int" => {
            let values: Vec<Option<i16>> = (0..n_rows)
                .map(|row_idx| {
                    pull_numeric_cell(stata_col_index, offset + row_idx + 1).map(|v| v as i16)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "long" => {
            let values: Vec<Option<i32>> = (0..n_rows)
                .map(|row_idx| {
                    pull_numeric_cell(stata_col_index, offset + row_idx + 1).map(|v| v as i32)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "float" => {
            let values: Vec<Option<f32>> = (0..n_rows)
                .map(|row_idx| {
                    pull_numeric_cell(stata_col_index, offset + row_idx + 1).map(|v| v as f32)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        _ => {
            let values: Vec<Option<f64>> = (0..n_rows)
                .map(|row_idx| pull_numeric_cell(stata_col_index, offset + row_idx + 1))
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
    }
}
