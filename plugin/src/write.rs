#![allow(clippy::too_many_arguments)]

use polars::prelude::*;
use polars_sql::SQLContext;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::create_dir_all;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::if_filter::{compile_if_expr, convert_if_sql};
use crate::metadata::{extract_dtmeta, DTMETA_KEY};
use crate::stata_interface::{
    count_rows, publish_transfer_metrics, pull_numeric_cell, pull_string_cell_with_buffer,
    pull_strl_cell_with_arena, read_macro, reset_transfer_metrics, set_macro, StrlArena,
};
use crate::utilities::{
    compute_pool_init_count, get_compute_thread_pool, get_io_thread_pool, io_pool_init_count,
    warm_thread_pools, write_pipeline_min_rows, write_pipeline_mode, write_pipeline_queue_capacity,
    AdaptiveBatchTuner, WritePipelineMode, STATA_DATE_ORIGIN, STATA_EPOCH_MS, TIME_MS,
};

fn publish_write_runtime_metrics(
    collect_calls: usize,
    planned_batches: usize,
    processed_batches: usize,
    elapsed_ms: u128,
) {
    set_macro("dtpq_write_collect_calls", &collect_calls.to_string(), true);
    set_macro(
        "dtpq_write_planned_batches",
        &planned_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_processed_batches",
        &processed_batches.to_string(),
        true,
    );
    set_macro("dtpq_write_elapsed_ms", &elapsed_ms.to_string(), true);
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
    publish_transfer_metrics("dtpq_write");
}

fn publish_write_batch_tuner_metrics(tuner: &AdaptiveBatchTuner) {
    set_macro(
        "dtpq_write_selected_batch_size",
        &tuner.selected_batch_size().to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_row_width_bytes",
        &tuner.row_width_bytes().to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_memory_cap_rows",
        &tuner.memory_guardrail_rows().to_string(),
        true,
    );
    set_macro(
        "dtpq_write_batch_adjustments",
        &tuner.tuning_adjustments().to_string(),
        true,
    );
    set_macro("dtpq_write_batch_tuner_mode", tuner.tuning_mode(), true);
}

fn publish_write_queue_metrics(
    mode: &str,
    queue_capacity: usize,
    queue_peak: usize,
    queue_backpressure_events: usize,
    queue_wait_ms: usize,
    produced_batches: usize,
    consumed_batches: usize,
) {
    set_macro("dtpq_write_pipeline_mode", mode, true);
    set_macro(
        "dtpq_write_queue_capacity",
        &queue_capacity.to_string(),
        true,
    );
    set_macro("dtpq_write_queue_peak", &queue_peak.to_string(), true);
    set_macro(
        "dtpq_write_queue_bp_events",
        &queue_backpressure_events.to_string(),
        true,
    );
    set_macro("dtpq_write_queue_wait_ms", &queue_wait_ms.to_string(), true);
    set_macro(
        "dtpq_write_queue_prod_batches",
        &produced_batches.to_string(),
        true,
    );
    set_macro(
        "dtpq_write_queue_cons_batches",
        &consumed_batches.to_string(),
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

#[derive(Debug, Deserialize)]
struct SchemaHandoff<T> {
    #[serde(alias = "v")]
    protocol_version: u32,
    #[serde(alias = "f")]
    fields: Vec<T>,
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

fn estimate_export_row_width_bytes(infos: &[ExportField]) -> usize {
    infos
        .iter()
        .map(|info| match info.dtype.as_str() {
            "byte" => 1,
            "int" => 2,
            "long" | "float" => 4,
            "double" => 8,
            "strl" => 128,
            _ if info.dtype.starts_with("str") => info.str_length.max(1) + 1,
            _ => 8,
        })
        .sum::<usize>()
        .max(1)
}

enum PipelineMessage {
    Batch(DataFrame),
    Error(String),
}

#[derive(Default)]
struct WriteQueueTelemetry {
    queue_depth: AtomicUsize,
    queue_peak: AtomicUsize,
    backpressure_events: AtomicUsize,
    queue_wait_ms: AtomicUsize,
    produced_batches: AtomicUsize,
}

fn update_queue_peak(telemetry: &WriteQueueTelemetry, depth: usize) {
    let mut peak = telemetry.queue_peak.load(Ordering::Relaxed);
    while depth > peak {
        match telemetry.queue_peak.compare_exchange_weak(
            peak,
            depth,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn send_pipeline_message(
    sender: &SyncSender<PipelineMessage>,
    telemetry: &WriteQueueTelemetry,
    stop_requested: &AtomicBool,
    message: PipelineMessage,
) -> bool {
    if stop_requested.load(Ordering::Relaxed) {
        return false;
    }

    match sender.try_send(message) {
        Ok(()) => {
            let depth = telemetry.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
            update_queue_peak(telemetry, depth);
            true
        }
        Err(TrySendError::Full(returned)) => {
            telemetry
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            let wait_started_at = Instant::now();
            match sender.send(returned) {
                Ok(()) => {
                    let wait_ms = wait_started_at.elapsed().as_millis() as usize;
                    telemetry
                        .queue_wait_ms
                        .fetch_add(wait_ms, Ordering::Relaxed);
                    let depth = telemetry.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
                    update_queue_peak(telemetry, depth);
                    true
                }
                Err(_) => false,
            }
        }
        Err(TrySendError::Disconnected(_)) => false,
    }
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
    pipeline_mode: WritePipelineMode,
    queue_capacity: usize,
    queue_telemetry: Arc<WriteQueueTelemetry>,
    stop_requested: Arc<AtomicBool>,
    pipeline_receiver: Option<Mutex<Receiver<PipelineMessage>>>,
    pipeline_worker: Mutex<Option<JoinHandle<()>>>,
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
            let dtype = match info.dtype.as_str() {
                "byte" => DataType::Int8,
                "int" => DataType::Int16,
                "long" => DataType::Int32,
                "float" => DataType::Float32,
                "double" => DataType::Float64,
                _ if info.dtype == "strl" || info.dtype.starts_with("str") => DataType::String,
                _ => DataType::Float64,
            };
            let dtype = if info.format.starts_with("%td") {
                DataType::Date
            } else if info.format.starts_with("%tc") {
                DataType::Datetime(TimeUnit::Milliseconds, None)
            } else {
                dtype
            };
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

        let requested_pipeline_mode = write_pipeline_mode();
        let pipeline_mode = if requested_pipeline_mode == WritePipelineMode::ProducerConsumer
            && n_rows >= write_pipeline_min_rows()
            && planned_batches > 1
        {
            WritePipelineMode::ProducerConsumer
        } else {
            WritePipelineMode::LegacyDirect
        };
        let queue_capacity = if pipeline_mode == WritePipelineMode::ProducerConsumer {
            let configured = write_pipeline_queue_capacity();
            let effective_max = planned_batches.saturating_sub(1).max(1);
            configured.min(effective_max)
        } else {
            0
        };

        let batch_size_hint = Arc::new(AtomicUsize::new(safe_batch_size));
        let queue_telemetry = Arc::new(WriteQueueTelemetry::default());
        let stop_requested = Arc::new(AtomicBool::new(false));

        let mut pipeline_receiver = None;
        let mut pipeline_worker = None;

        if pipeline_mode == WritePipelineMode::ProducerConsumer && n_rows > 0 {
            let (sender, receiver) = sync_channel(queue_capacity);
            pipeline_receiver = Some(Mutex::new(receiver));

            let producer_columns = column_info.clone();
            let producer_batch_size_hint = batch_size_hint.clone();
            let producer_batch_tuner = batch_tuner.clone();
            let producer_telemetry = queue_telemetry.clone();
            let producer_stop_requested = stop_requested.clone();

            pipeline_worker = Some(thread::spawn(move || {
                let mut producer_offset = 0usize;
                while producer_offset < n_rows {
                    if producer_stop_requested.load(Ordering::Relaxed) {
                        break;
                    }

                    let requested_batch_size =
                        producer_batch_size_hint.load(Ordering::Relaxed).max(1);
                    let read_count = std::cmp::min(requested_batch_size, n_rows - producer_offset);
                    let batch_started_at = Instant::now();

                    let batch_df_result = read_batch_from_columns(
                        &producer_columns,
                        start_row + producer_offset,
                        read_count,
                    );

                    let batch_df = match batch_df_result {
                        Ok(df) => df,
                        Err(err) => {
                            let _ = send_pipeline_message(
                                &sender,
                                &producer_telemetry,
                                &producer_stop_requested,
                                PipelineMessage::Error(err.to_string()),
                            );
                            break;
                        }
                    };

                    let batch_rows = batch_df.height();
                    if batch_rows == 0 {
                        break;
                    }

                    let next_batch_size = {
                        let mut tuner = producer_batch_tuner.lock().unwrap();
                        tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis())
                    };
                    producer_batch_size_hint.store(next_batch_size.max(1), Ordering::Relaxed);

                    if !send_pipeline_message(
                        &sender,
                        &producer_telemetry,
                        &producer_stop_requested,
                        PipelineMessage::Batch(batch_df),
                    ) {
                        break;
                    }

                    producer_telemetry
                        .produced_batches
                        .fetch_add(1, Ordering::Relaxed);
                    producer_offset += batch_rows;
                }
            }));
        }

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
            pipeline_mode,
            queue_capacity,
            queue_telemetry,
            stop_requested,
            pipeline_receiver,
            pipeline_worker: Mutex::new(pipeline_worker),
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
        if self.pipeline_mode == WritePipelineMode::ProducerConsumer {
            "producer_consumer"
        } else {
            "legacy_direct"
        }
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
        if self.pipeline_mode == WritePipelineMode::ProducerConsumer {
            self.queue_telemetry
                .produced_batches
                .load(Ordering::Relaxed)
        } else {
            self.processed_batches()
        }
    }

    pub fn queue_consumed_batches(&self) -> usize {
        self.processed_batches()
    }

    fn join_pipeline_worker(&self) {
        if let Some(handle) = self.pipeline_worker.lock().unwrap().take() {
            let _ = handle.join();
        }
    }

    fn queue_message_consumed(&self) {
        let depth = self.queue_telemetry.queue_depth.load(Ordering::Relaxed);
        if depth > 0 {
            self.queue_telemetry
                .queue_depth
                .fetch_sub(1, Ordering::Relaxed);
        }
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

    fn next_batch_pipeline(&self) -> PolarsResult<Option<DataFrame>> {
        let receiver = match self.pipeline_receiver.as_ref() {
            Some(r) => r,
            None => return Ok(None),
        };
        let received = {
            let receiver_guard = receiver.lock().unwrap();
            receiver_guard.recv()
        };

        match received {
            Ok(PipelineMessage::Batch(df)) => {
                self.queue_message_consumed();
                self.processed_batches.fetch_add(1, Ordering::Relaxed);
                Ok(Some(df))
            }
            Ok(PipelineMessage::Error(message)) => {
                self.queue_message_consumed();
                self.stop_requested.store(true, Ordering::Relaxed);
                self.join_pipeline_worker();
                Err(PolarsError::ComputeError(message.into()))
            }
            Err(_) => {
                self.join_pipeline_worker();
                Ok(None)
            }
        }
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

    fn scan_pipeline(&self) -> PolarsResult<DataFrame> {
        let mut combined: Option<DataFrame> = None;
        while let Some(batch) = self.next_batch_pipeline()? {
            if let Some(existing) = combined.as_mut() {
                existing.vstack_mut(&batch)?;
            } else {
                combined = Some(batch);
            }
        }
        match combined {
            Some(df) => Ok(df),
            None => Ok(DataFrame::empty_with_schema(&self.schema)),
        }
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
        if self.pipeline_mode == WritePipelineMode::ProducerConsumer {
            self.scan_pipeline()
        } else {
            self.scan_legacy()
        }
    }

    fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        if self.pipeline_mode == WritePipelineMode::ProducerConsumer {
            self.next_batch_pipeline()
        } else {
            self.next_batch_legacy()
        }
    }
}

impl Drop for StataRowSource {
    fn drop(&mut self) {
        self.stop_requested.store(true, Ordering::Relaxed);
        if let Ok(worker_slot) = self.pipeline_worker.get_mut() {
            if let Some(handle) = worker_slot.take() {
                let _ = handle.join();
            }
        }
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

        if info.dtype == "strl" || info.dtype.starts_with("str") {
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
    warm_thread_pools();
    reset_transfer_metrics();
    publish_write_runtime_metrics(0, 0, 0, 0);
    set_macro("dtpq_write_selected_batch_size", "0", true);
    set_macro("dtpq_write_batch_row_width_bytes", "0", true);
    set_macro("dtpq_write_batch_memory_cap_rows", "0", true);
    set_macro("dtpq_write_batch_adjustments", "0", true);
    set_macro("dtpq_write_batch_tuner_mode", "fixed", true);
    set_macro("dtpq_if_filter_mode", "none", true);
    set_macro("dtpq_write_schema_handoff", "legacy_macros", true);
    publish_write_queue_metrics("legacy_direct", 0, 0, 0, 0, 0, 0);

    let selected_vars_owned;
    let selected_vars = if varlist.is_empty() || varlist == "from_macro" {
        selected_vars_owned = read_macro("varlist", false, Some(10 * 1024 * 1024));
        selected_vars_owned.as_str()
    } else {
        varlist
    };

    let all_columns = if mapping.is_empty() || mapping == "from_macros" {
        let var_count = read_macro("var_count", false, None).parse::<usize>()?;
        column_info_from_macros(var_count)
    } else {
        let (fields, handoff_mode) = parse_schema_handoff_fields::<ExportField>(mapping, "save")?;
        set_macro("dtpq_write_schema_handoff", handoff_mode, true);
        fields
    };

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

    let scan = Arc::new(StataRowSource::new(
        selected_infos,
        start_row,
        rows_to_read,
        batch_size,
        row_width_bytes,
    ));
    let mut lf = LazyFrame::anonymous_scan(scan.clone(), ScanArgsAnonymous::default())?;

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

    let partition_cols: Vec<PlSmallStr> = partition_by
        .split_whitespace()
        .map(PlSmallStr::from)
        .collect();

    let dtmeta_json = extract_dtmeta();

    if partition_cols.is_empty() {
        write_single_dataframe(
            path,
            lf,
            compression,
            compression_level,
            overwrite_partition,
            &dtmeta_json,
            &mut collect_calls,
        )?;
    } else {
        collect_calls += 1;
        let mut df = lf.collect()?;
        write_partitioned_dataframe(
            path,
            &mut df,
            compression,
            compression_level,
            &partition_cols,
            overwrite_partition,
            &dtmeta_json,
        )?;
    }

    scan.join_pipeline_worker();

    let batch_tuner = scan.batch_tuner_snapshot();
    publish_write_batch_tuner_metrics(&batch_tuner);
    publish_write_queue_metrics(
        scan.pipeline_mode_name(),
        scan.queue_capacity(),
        scan.queue_peak(),
        scan.queue_backpressure_events(),
        scan.queue_wait_ms(),
        scan.queue_produced_batches(),
        scan.queue_consumed_batches(),
    );

    publish_write_runtime_metrics(
        collect_calls,
        scan.planned_batches(),
        scan.processed_batches(),
        started_at.elapsed().as_millis(),
    );

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

fn column_info_from_macros(n_vars: usize) -> Vec<ExportField> {
    (1..=n_vars)
        .map(|i| ExportField {
            name: read_macro(&format!("name_{}", i), false, None),
            dtype: read_macro(&format!("dtype_{}", i), false, None).to_lowercase(),
            format: read_macro(&format!("format_{}", i), false, None).to_lowercase(),
            str_length: read_macro(&format!("str_length_{}", i), false, None)
                .parse::<usize>()
                .unwrap_or(0),
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

    if info.dtype.starts_with("str") {
        let width = info.str_length.max(1);
        let mut str_buffer: Vec<i8> = vec![0; width.saturating_add(1)];
        let values: Vec<String> = (0..n_rows)
            .map(|row_idx| {
                pull_string_cell_with_buffer(stata_col_index, offset + row_idx + 1, &mut str_buffer)
            })
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if info.format.starts_with("%td") {
        let values: Vec<Option<i32>> = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i32 - STATA_DATE_ORIGIN)
            })
            .collect();
        return Series::new((&info.name).into(), values).cast(&DataType::Date);
    }

    if info.format.starts_with("%tc") {
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
