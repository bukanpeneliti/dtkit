use glob::glob;
use polars::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;
use walkdir::WalkDir;

pub use stata_sys::{
    display, set_macro, SF_error, SF_is_missing, SF_nobs, SF_nvar, SF_sdata, SF_sdatalen,
    SF_strldata, SF_vdata,
};

// --- Config Constants ---

pub const SCHEMA_HANDOFF_PROTOCOL_VERSION: u32 = 2;
pub const SCHEMA_VALIDATION_SAMPLE_ROWS: usize = 100;
pub const DEFAULT_BATCH_SIZE: usize = 50_000;

pub const ENV_DTPARQUET_THREADS: &str = "DTPARQUET_THREADS";
pub const ENV_POLARS_MAX_THREADS: &str = "POLARS_MAX_THREADS";
pub const ENV_BATCH_AUTOTUNE_MODE: &str = "DTPARQUET_BATCH_AUTOTUNE_MODE";
pub const ENV_BATCH_MEMORY_MB: &str = "DTPARQUET_BATCH_MEMORY_MB";
pub const ENV_BATCH_MIN_ROWS: &str = "DTPARQUET_BATCH_MIN_ROWS";
pub const ENV_BATCH_MAX_ROWS: &str = "DTPARQUET_BATCH_MAX_ROWS";
pub const ENV_BATCH_TARGET_MS: &str = "DTPARQUET_BATCH_TARGET_MS";
pub const ENV_WRITE_PIPELINE_MODE: &str = "DTPARQUET_WRITE_PIPELINE_MODE";
pub const ENV_WRITE_PIPELINE_QUEUE_CAPACITY: &str = "DTPARQUET_WRITE_PIPELINE_QUEUE_CAPACITY";
pub const ENV_WRITE_PIPELINE_MIN_ROWS: &str = "DTPARQUET_WRITE_PIPELINE_MIN_ROWS";
pub const ENV_LAZY_EXECUTION_MODE: &str = "DTPARQUET_LAZY_EXECUTION_MODE";

pub const DEFAULT_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 8;
pub const MAX_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 32;
pub const DEFAULT_WRITE_PIPELINE_MIN_ROWS: usize = 20_000;
pub const DEFAULT_MEMORY_BUDGET_MB: usize = 512;
pub const ROW_ESTIMATE_BYTES: usize = 64;
pub const MIN_BATCH_SIZE: usize = 1_000;
pub const MAX_BATCH_SIZE: usize = 100_000;
pub const DEFAULT_BATCH_MIN_ROWS: usize = 10_000;
pub const DEFAULT_BATCH_MAX_ROWS: usize = 250_000;
pub const DEFAULT_BATCH_TARGET_MS: usize = 200;

// --- Stata Interface ---

#[allow(non_camel_case_types)]
pub type ST_retcode = i32;

static REPLACE_NUMBER_CALLS: AtomicU64 = AtomicU64::new(0);
static REPLACE_STRING_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_NUMERIC_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_STRING_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_STRL_CALLS: AtomicU64 = AtomicU64::new(0);
static STRL_TRUNC_EVENTS: AtomicU64 = AtomicU64::new(0);
static STRL_BINARY_EVENTS: AtomicU64 = AtomicU64::new(0);
static TRANSFER_FALLBACK_CALLS: AtomicU64 = AtomicU64::new(0);
static TRANSFER_CONVERSION_FAILURES: AtomicU64 = AtomicU64::new(0);

pub fn reset_transfer_metrics() {
    [
        &REPLACE_NUMBER_CALLS,
        &REPLACE_STRING_CALLS,
        &PULL_NUMERIC_CALLS,
        &PULL_STRING_CALLS,
        &PULL_STRL_CALLS,
        &STRL_TRUNC_EVENTS,
        &STRL_BINARY_EVENTS,
        &TRANSFER_FALLBACK_CALLS,
        &TRANSFER_CONVERSION_FAILURES,
    ]
    .iter()
    .for_each(|a| a.store(0, Ordering::Relaxed));
}

pub fn publish_transfer_metrics(prefix: &str) {
    let rn = REPLACE_NUMBER_CALLS.load(Ordering::Relaxed);
    let rs = REPLACE_STRING_CALLS.load(Ordering::Relaxed);
    let pn = PULL_NUMERIC_CALLS.load(Ordering::Relaxed);
    let ps = PULL_STRING_CALLS.load(Ordering::Relaxed);
    let pl = PULL_STRL_CALLS.load(Ordering::Relaxed);
    let total = rn + rs + pn + ps + pl;

    let m = [
        ("replace_number_calls", rn),
        ("replace_string_calls", rs),
        ("pull_numeric_calls", pn),
        ("pull_string_calls", ps),
        ("pull_strl_calls", pl),
        (
            "strl_trunc_events",
            STRL_TRUNC_EVENTS.load(Ordering::Relaxed),
        ),
        (
            "strl_binary_events",
            STRL_BINARY_EVENTS.load(Ordering::Relaxed),
        ),
        ("transfer_calls_total", total),
        (
            "fallback_calls",
            TRANSFER_FALLBACK_CALLS.load(Ordering::Relaxed),
        ),
        (
            "conversion_failures",
            TRANSFER_CONVERSION_FAILURES.load(Ordering::Relaxed),
        ),
    ];
    for (name, val) in m {
        set_macro(&format!("{prefix}_{name}"), &val.to_string(), true);
    }
}

pub fn read_macro(name: &str, global: bool, buffer_size: Option<usize>) -> String {
    stata_sys::get_macro(name, global, buffer_size).unwrap_or_default()
}

pub fn replace_number(value: Option<f64>, row: usize, col: usize) -> i32 {
    REPLACE_NUMBER_CALLS.fetch_add(1, Ordering::Relaxed);
    stata_sys::replace_number(value, row, col)
}

pub fn replace_string(value: Option<String>, row: usize, col: usize) -> i32 {
    REPLACE_STRING_CALLS.fetch_add(1, Ordering::Relaxed);
    stata_sys::replace_string(value, row, col)
}

pub fn record_transfer_conversion_failure() {
    TRANSFER_CONVERSION_FAILURES.fetch_add(1, Ordering::Relaxed);
}
pub fn count_stata_rows() -> i32 {
    unsafe { SF_nobs() }
}

pub fn pull_numeric_cell(col: usize, row: usize) -> Option<f64> {
    PULL_NUMERIC_CALLS.fetch_add(1, Ordering::Relaxed);
    let mut val: f64 = 0.0;
    unsafe {
        if SF_vdata(col as i32, row as i32, &mut val) != 0 || SF_is_missing(val) {
            None
        } else {
            Some(val)
        }
    }
}

pub fn pull_string_cell_with_buffer(col: usize, row: usize, buffer: &mut Vec<i8>) -> String {
    use std::ffi::{c_char, CStr};
    PULL_STRING_CALLS.fetch_add(1, Ordering::Relaxed);
    unsafe {
        if !buffer.is_empty() {
            buffer[0] = 0;
        }
        SF_sdata(col as i32, row as i32, buffer.as_mut_ptr() as *mut c_char);
        CStr::from_ptr(buffer.as_ptr() as *const c_char)
            .to_string_lossy()
            .into_owned()
    }
}

#[derive(Debug, Default)]
pub struct StrlArena {
    buffer: Vec<u8>,
}
impl StrlArena {
    pub fn new() -> Self {
        Self::default()
    }
    fn reserve(&mut self, len: usize) {
        if self.buffer.len() < len {
            self.buffer.resize((len.div_ceil(16384)) * 16384, 0);
        }
    }
}

pub fn pull_strl_cell_with_arena(
    col: usize,
    row: usize,
    arena: &mut StrlArena,
) -> Result<String, ()> {
    use std::ffi::c_char;
    PULL_STRL_CALLS.fetch_add(1, Ordering::Relaxed);
    unsafe {
        let len = SF_sdatalen(col as i32, row as i32);
        if len < 0 {
            return Err(());
        }
        let len_u = len as usize;
        arena.reserve(len_u + 1);
        let buf = &mut arena.buffer[..len_u + 1];
        SF_strldata(
            col as i32,
            row as i32,
            buf.as_mut_ptr() as *mut c_char,
            len + 1,
        );
        let end = buf[..len_u].iter().position(|&b| b == 0).unwrap_or(len_u);
        if end < len_u {
            STRL_TRUNC_EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        let res = String::from_utf8_lossy(&buf[..end]).into_owned();
        if res.as_bytes().len() != end {
            STRL_BINARY_EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(res)
    }
}

// --- Mapping ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    #[serde(alias = "i")]
    pub index: usize,
    #[serde(alias = "n")]
    pub name: String,
    #[serde(alias = "d")]
    pub dtype: String,
    #[serde(alias = "s")]
    pub stata_type: String,
}

#[derive(Clone, Debug, Deserialize)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransferWriterKind {
    Numeric,
    Date,
    Time,
    Datetime,
    String,
    Strl,
}

pub fn transfer_writer_kind_from_stata_type(stata_type: &str) -> TransferWriterKind {
    match stata_type {
        "string" => TransferWriterKind::String,
        "strl" => TransferWriterKind::Strl,
        "date" => TransferWriterKind::Date,
        "time" => TransferWriterKind::Time,
        "datetime" => TransferWriterKind::Datetime,
        _ => TransferWriterKind::Numeric,
    }
}

pub fn is_stata_string_dtype(dtype: &str) -> bool {
    dtype == "strl" || dtype.starts_with("str")
}
pub fn is_stata_date_format(f: &str) -> bool {
    f.starts_with("%td")
}
pub fn is_stata_datetime_format(f: &str) -> bool {
    f.starts_with("%tc")
}

pub fn export_field_polars_dtype(dtype: &str, format: &str) -> DataType {
    if is_stata_date_format(format) {
        return DataType::Date;
    }
    if is_stata_datetime_format(format) {
        return DataType::Datetime(TimeUnit::Milliseconds, None);
    }
    match dtype {
        "byte" => DataType::Int8,
        "int" => DataType::Int16,
        "long" => DataType::Int32,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        _ if is_stata_string_dtype(dtype) => DataType::String,
        _ => DataType::Float64,
    }
}

pub fn estimate_export_field_width_bytes(dtype: &str, len: usize) -> usize {
    match dtype {
        "byte" => 1,
        "int" => 2,
        "long" | "float" => 4,
        "double" => 8,
        "strl" => 128,
        _ if is_stata_string_dtype(dtype) => len.max(1) + 1,
        _ => 8,
    }
}

pub fn stata_to_polars_type(stata_type: &str) -> DataType {
    match stata_type {
        "byte" => DataType::Int8,
        "int" => DataType::Int16,
        "long" => DataType::Int32,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        _ => DataType::String,
    }
}

// --- Thread Pools ---

pub const STATA_DATE_ORIGIN: i32 = 3653;
pub const STATA_EPOCH_MS: i64 = 315619200;
pub const TIME_MS: i64 = 1_000;
pub const TIME_US: i64 = 1_000_000;
pub const TIME_NS: i64 = 1_000_000_000;

static IO_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
static COMPUTE_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
static IO_POOL_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static COMPUTE_POOL_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

fn get_env_u(key: &str) -> Option<usize> {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
}
fn get_hw_threads() -> usize {
    thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

pub fn get_io_thread_pool() -> &'static ThreadPool {
    IO_THREAD_POOL.get_or_init(|| {
        IO_POOL_INIT_COUNT.fetch_add(1, Ordering::Relaxed);
        ThreadPoolBuilder::new()
            .num_threads(get_hw_threads().clamp(2, 8))
            .thread_name(|i| format!("dtpq-io-{i}"))
            .build()
            .unwrap()
    })
}

pub fn get_compute_thread_pool() -> &'static ThreadPool {
    COMPUTE_THREAD_POOL.get_or_init(|| {
        COMPUTE_POOL_INIT_COUNT.fetch_add(1, Ordering::Relaxed);
        let n = get_env_u(ENV_DTPARQUET_THREADS)
            .or_else(|| get_env_u(ENV_POLARS_MAX_THREADS))
            .unwrap_or_else(get_hw_threads);
        ThreadPoolBuilder::new()
            .num_threads(n.max(1))
            .thread_name(|i| format!("dtpq-cpu-{i}"))
            .build()
            .unwrap()
    })
}

pub fn warm_thread_pools() {
    let _ = get_compute_thread_pool();
    let _ = get_io_thread_pool();
}
pub fn io_pool_init_count() -> usize {
    IO_POOL_INIT_COUNT.load(Ordering::Relaxed)
}
pub fn compute_pool_init_count() -> usize {
    COMPUTE_POOL_INIT_COUNT.load(Ordering::Relaxed)
}

#[derive(Copy, Clone, Debug)]
pub enum BatchMode {
    ByRow,
    ByColumn,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WritePipelineMode {
    ProducerConsumer,
    LegacyDirect,
}

pub fn write_pipeline_mode() -> WritePipelineMode {
    env::var(ENV_WRITE_PIPELINE_MODE)
        .map(|raw| {
            if matches!(
                raw.trim().to_lowercase().as_str(),
                "legacy" | "direct" | "off" | "fixed"
            ) {
                WritePipelineMode::LegacyDirect
            } else {
                WritePipelineMode::ProducerConsumer
            }
        })
        .unwrap_or(WritePipelineMode::LegacyDirect)
}

pub fn determine_parallelization_strategy(n_cols: usize, n_rows: usize, cores: usize) -> BatchMode {
    if n_cols > cores * 2 && n_rows < 50_000 {
        BatchMode::ByColumn
    } else {
        BatchMode::ByRow
    }
}

// --- Metrics ---

pub struct CommonRuntimeMetrics {
    pub collect_calls: usize,
    pub planned_batches: usize,
    pub processed_batches: usize,
    pub elapsed_ms: u128,
    pub compute_pool_threads: usize,
    pub compute_pool_inits: usize,
    pub io_pool_threads: usize,
    pub io_pool_inits: usize,
}

impl CommonRuntimeMetrics {
    pub fn zero() -> Self {
        Self {
            collect_calls: 0,
            planned_batches: 0,
            processed_batches: 0,
            elapsed_ms: 0,
            compute_pool_threads: get_compute_thread_pool().current_num_threads(),
            compute_pool_inits: 0,
            io_pool_threads: get_io_thread_pool().current_num_threads(),
            io_pool_inits: 0,
        }
    }
    pub fn collect(&mut self, start: Instant) {
        self.elapsed_ms = start.elapsed().as_millis();
        self.compute_pool_threads = get_compute_thread_pool().current_num_threads();
        self.compute_pool_inits = compute_pool_init_count();
        self.io_pool_threads = get_io_thread_pool().current_num_threads();
        self.io_pool_inits = io_pool_init_count();
    }
    pub fn emit_to_macros(&self, prefix: &str) {
        let m = [
            ("collect_calls", self.collect_calls.to_string()),
            ("planned_batches", self.planned_batches.to_string()),
            ("processed_batches", self.processed_batches.to_string()),
            ("elapsed_ms", self.elapsed_ms.to_string()),
            (
                "compute_pool_threads",
                self.compute_pool_threads.to_string(),
            ),
            ("compute_pool_inits", self.compute_pool_inits.to_string()),
            ("io_pool_threads", self.io_pool_threads.to_string()),
            ("io_pool_inits", self.io_pool_inits.to_string()),
        ];
        for (n, v) in m {
            set_macro(&format!("{prefix}_{n}"), &v, true);
        }
        set_macro(
            "compute_pool_inits",
            &self.compute_pool_inits.to_string(),
            true,
        );
        set_macro("io_pool_inits", &self.io_pool_inits.to_string(), true);
        publish_transfer_metrics(prefix);
    }
}

pub struct CommonBatchTunerMetrics {
    pub selected_batch_size: usize,
    pub row_width_bytes: usize,
    pub memory_cap_rows: usize,
    pub adjustments: usize,
    pub tuner_mode: &'static str,
}
impl CommonBatchTunerMetrics {
    pub fn from_tuner(t: &AdaptiveBatchTuner) -> Self {
        Self {
            selected_batch_size: t.selected_batch_size(),
            row_width_bytes: t.row_width_bytes(),
            memory_cap_rows: t.memory_guardrail_rows(),
            adjustments: t.tuning_adjustments(),
            tuner_mode: t.tuning_mode(),
        }
    }
    pub fn emit_to_macros(&self, prefix: &str) {
        let m = [
            ("selected_batch_size", self.selected_batch_size.to_string()),
            ("batch_row_width_bytes", self.row_width_bytes.to_string()),
            ("batch_memory_cap_rows", self.memory_cap_rows.to_string()),
            ("batch_adjustments", self.adjustments.to_string()),
            ("batch_tuner_mode", self.tuner_mode.to_string()),
        ];
        for (n, v) in m {
            set_macro(&format!("{prefix}_{n}"), &v, true);
        }
    }
}

// --- Adaptive Batch Tuner ---

#[derive(Clone, Debug)]
pub struct AdaptiveBatchTuner {
    min_batch: usize,
    max_batch: usize,
    row_width: usize,
    mem_rows: usize,
    target_ms: f64,
    curr_batch: usize,
    smoothed_rpms: f64,
    adjustments: usize,
    autotune: bool,
}

impl AdaptiveBatchTuner {
    pub fn new(row_width: usize, conf_batch: usize, def_batch: usize) -> Self {
        let row_width = row_width.max(1);
        let budget_mb = get_env_u(ENV_BATCH_MEMORY_MB)
            .unwrap_or(DEFAULT_MEMORY_BUDGET_MB)
            .max(1);
        let mem_rows = (budget_mb * 1024 * 1024 / row_width).max(1);
        let max_b = get_env_u(ENV_BATCH_MAX_ROWS)
            .unwrap_or(DEFAULT_BATCH_MAX_ROWS)
            .min(mem_rows)
            .min(if conf_batch == 0 {
                usize::MAX
            } else {
                conf_batch
            })
            .max(1);
        let min_b = get_env_u(ENV_BATCH_MIN_ROWS)
            .unwrap_or(DEFAULT_BATCH_MIN_ROWS)
            .min(max_b)
            .max(1);
        let seed = (budget_mb * 1024 * 1024 / row_width.max(ROW_ESTIMATE_BYTES / 10))
            .clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
        let start_b = (if conf_batch > 0 {
            conf_batch
        } else if def_batch > 0 {
            seed.max(def_batch)
        } else {
            seed
        })
        .clamp(min_b, max_b);
        Self {
            min_batch: min_b,
            max_batch: max_b,
            row_width,
            mem_rows,
            target_ms: get_env_u(ENV_BATCH_TARGET_MS).unwrap_or(DEFAULT_BATCH_TARGET_MS) as f64,
            curr_batch: start_b,
            smoothed_rpms: 0.0,
            adjustments: 0,
            autotune: env::var(ENV_BATCH_AUTOTUNE_MODE)
                .map(|m| {
                    !matches!(
                        m.trim().to_lowercase().as_str(),
                        "fixed" | "off" | "static" | "legacy"
                    )
                })
                .unwrap_or(true),
        }
    }
    pub fn selected_batch_size(&self) -> usize {
        self.curr_batch
    }
    pub fn row_width_bytes(&self) -> usize {
        self.row_width
    }
    pub fn memory_guardrail_rows(&self) -> usize {
        self.mem_rows
    }
    pub fn tuning_adjustments(&self) -> usize {
        self.adjustments
    }
    pub fn tuning_mode(&self) -> &'static str {
        if self.autotune {
            "adaptive"
        } else {
            "fixed"
        }
    }
    pub fn observe_batch(&mut self, rows: usize, ms: u128) -> usize {
        if rows == 0 || !self.autotune {
            return self.curr_batch;
        }
        let rpms = rows as f64 / (ms as f64).max(1.0);
        self.smoothed_rpms = if self.smoothed_rpms == 0.0 {
            rpms
        } else {
            self.smoothed_rpms * 0.7 + rpms * 0.3
        };
        let target = (self.smoothed_rpms * self.target_ms).round() as usize;
        let mut next = (self.curr_batch * 3 + target) / 4;
        if ms as f64 > self.target_ms * 1.35 {
            next = next * 9 / 10;
        } else if (ms as f64) < self.target_ms * 0.65 {
            next = next * 11 / 10;
        }
        let unit = if next >= 50000 {
            5000
        } else if next >= 10000 {
            1000
        } else {
            100
        };
        let rounded = ((next + unit / 2) / unit).max(1) * unit;
        let clamped = rounded.clamp(self.min_batch, self.max_batch);
        if clamped != self.curr_batch {
            self.adjustments += 1;
            self.curr_batch = clamped;
        }
        self.curr_batch
    }
}

// --- Metadata ---

pub const DTMETA_KEY: &str = "dtparquet.dtmeta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtMeta {
    pub schema_version: i32,
    pub min_reader_version: i32,
    pub vars: Vec<VarMeta>,
    pub value_labels: Vec<ValueLabelMeta>,
    pub dta_label: String,
    #[serde(default)]
    pub dta_obs: i64,
    #[serde(default)]
    pub dta_vars: i64,
    #[serde(default)]
    pub dta_ts: String,
    #[serde(default)]
    pub dta_notes: Vec<String>,
    #[serde(default)]
    pub var_notes: Vec<VarNoteMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarMeta {
    pub name: String,
    pub stata_type: String,
    pub format: String,
    pub var_label: String,
    pub value_label: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueLabelMeta {
    pub name: String,
    pub value: i64,
    pub text: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarNoteMeta {
    pub varname: String,
    pub text: String,
}

pub fn extract_dtmeta() -> String {
    let get_u = |n: &str| read_macro(n, false, None).parse().unwrap_or(0usize);
    let get_i = |n: &str| read_macro(n, false, None).parse().unwrap_or(0i64);
    let vars = (1..=get_u("dtmeta_var_count"))
        .map(|i| VarMeta {
            name: read_macro(&format!("dtmeta_varname_{i}"), false, None),
            stata_type: read_macro(&format!("dtmeta_vartype_{i}"), false, None),
            format: read_macro(&format!("dtmeta_varfmt_{i}"), false, None),
            var_label: read_macro(&format!("dtmeta_varlab_{i}"), false, Some(65536)),
            value_label: read_macro(&format!("dtmeta_vallab_{i}"), false, None),
        })
        .collect();
    let labels = (1..=get_u("dtmeta_label_count"))
        .map(|i| ValueLabelMeta {
            name: read_macro(&format!("dtmeta_label_name_{i}"), false, None),
            value: get_i(&format!("dtmeta_label_value_{i}")),
            text: read_macro(&format!("dtmeta_label_text_{i}"), false, Some(65536)),
        })
        .collect();
    let meta = DtMeta {
        schema_version: 1,
        min_reader_version: 1,
        vars,
        value_labels: labels,
        dta_label: read_macro("dtmeta_dta_label", false, Some(65536)),
        dta_obs: get_i("dtmeta_dta_obs"),
        dta_vars: get_i("dtmeta_dta_vars"),
        dta_ts: read_macro("dtmeta_dta_ts", false, Some(65536)),
        dta_notes: (1..=get_u("dtmeta_dta_note_count"))
            .map(|i| read_macro(&format!("dtmeta_dta_note_{i}"), false, Some(65536)))
            .collect(),
        var_notes: (1..=get_u("dtmeta_var_note_count"))
            .map(|i| VarNoteMeta {
                varname: read_macro(&format!("dtmeta_var_note_var_{i}"), false, None),
                text: read_macro(&format!("dtmeta_var_note_text_{i}"), false, Some(65536)),
            })
            .collect(),
    };
    serde_json::to_string(&meta).unwrap_or_default()
}

pub fn load_dtmeta_from_parquet(path: &str) -> Option<DtMeta> {
    let mut r = ParquetReader::new(File::open(path).ok()?);
    let kv = r.get_metadata().ok()?.key_value_metadata().as_ref()?;
    serde_json::from_str(kv.iter().find(|e| e.key == DTMETA_KEY)?.value.as_deref()?).ok()
}

pub fn has_parquet_metadata_key(path: &str, key: &str) -> Result<bool, Box<dyn Error>> {
    let mut r = ParquetReader::new(File::open(path)?);
    Ok(r.get_metadata()?
        .key_value_metadata()
        .as_ref()
        .map(|kv| kv.iter().any(|e| e.key == key))
        .unwrap_or(false))
}

pub fn expose_dtmeta_to_macros(m: &DtMeta) {
    set_macro("dtmeta_var_count", &m.vars.len().to_string(), false);
    for (i, v) in m.vars.iter().enumerate() {
        let x = i + 1;
        set_macro(&format!("dtmeta_varname_{x}"), &v.name, false);
        set_macro(&format!("dtmeta_vartype_{x}"), &v.stata_type, false);
        set_macro(&format!("dtmeta_varfmt_{x}"), &v.format, false);
        set_macro(&format!("dtmeta_varlab_{x}"), &v.var_label, false);
        set_macro(&format!("dtmeta_vallab_{x}"), &v.value_label, false);
    }
    set_macro(
        "dtmeta_label_count",
        &m.value_labels.len().to_string(),
        false,
    );
    for (i, l) in m.value_labels.iter().enumerate() {
        let x = i + 1;
        set_macro(&format!("dtmeta_label_name_{x}"), &l.name, false);
        set_macro(
            &format!("dtmeta_label_value_{x}"),
            &l.value.to_string(),
            false,
        );
        set_macro(&format!("dtmeta_label_text_{x}"), &l.text, false);
    }
    set_macro("dtmeta_dta_label", &m.dta_label, false);
    set_macro("dtmeta_dta_obs", &m.dta_obs.to_string(), false);
    set_macro("dtmeta_dta_vars", &m.dta_vars.to_string(), false);
    set_macro("dtmeta_dta_ts", &m.dta_ts, false);
    set_macro(
        "dtmeta_dta_note_count",
        &m.dta_notes.len().to_string(),
        false,
    );
    for (i, n) in m.dta_notes.iter().enumerate() {
        set_macro(&format!("dtmeta_dta_note_{}", i + 1), n, false);
    }
    set_macro(
        "dtmeta_var_note_count",
        &m.var_notes.len().to_string(),
        false,
    );
    for (i, n) in m.var_notes.iter().enumerate() {
        let x = i + 1;
        set_macro(&format!("dtmeta_var_note_var_{x}"), &n.varname, false);
        set_macro(&format!("dtmeta_var_note_text_{x}"), &n.text, false);
    }
}

// --- Schema ---

pub fn validate_parquet_schema(path: &str, exp: &[&str]) -> Result<(), String> {
    let mut r = ParquetReader::new(File::open(path).map_err(|e| e.to_string())?);
    let s = r.schema().map_err(|e| format!("{e:?}"))?;
    let p_cols: HashSet<&str> = s.iter_names().map(|s| s.as_str()).collect();
    let miss: Vec<_> = exp
        .iter()
        .filter(|c| !p_cols.contains(*c))
        .copied()
        .collect();
    if !miss.is_empty() {
        return Err(format!("Missing columns: {miss:?}"));
    }
    Ok(())
}

pub fn set_schema_macros(
    s: &Schema,
    lens: &HashMap<String, usize>,
    det: bool,
    q: bool,
) -> PolarsResult<usize> {
    if !q {
        display("Variable Name                    | Polars Type                      | Stata Type");
        display("-------------------------------- | -------------------------------- | ----------");
    }
    set_macro(
        "schema_protocol_version",
        &SCHEMA_HANDOFF_PROTOCOL_VERSION.to_string(),
        false,
    );
    let mut fields = Vec::with_capacity(s.len());
    for (i, (n, dt)) in s.iter().enumerate() {
        let pt = match dt {
            DataType::Boolean | DataType::Int8 => "int8",
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
            DataType::Time | DataType::Datetime(_, _) => "int64",
            DataType::String => "string",
            DataType::Categorical(_, _) => "categorical",
            DataType::Enum(_, _) => "enum",
            DataType::Binary => "binary",
            _ => "string",
        };
        let sl = if det {
            *lens.get(n.as_str()).unwrap_or(&0)
        } else if dt.is_string() || matches!(dt, DataType::Categorical(_, _) | DataType::Enum(_, _))
        {
            2045
        } else {
            0
        };
        let st = match dt {
            DataType::Boolean | DataType::Int8 => "byte",
            DataType::Int16 => "int",
            DataType::Int32 => "long",
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 | DataType::Float64 => "double",
            DataType::UInt8 => "int",
            DataType::UInt16 => "long",
            DataType::Float32 => "float",
            DataType::Date => "date",
            DataType::Time => "time",
            DataType::Datetime(_, _) => "datetime",
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _)
                if det && sl > 2045 =>
            {
                "strl"
            }
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => "string",
            _ => "strl",
        };
        if !q {
            display(&format!("{n:<32} | {dt:<32?} | {st}"));
        }
        let x = i + 1;
        set_macro(&format!("name_{x}"), n, false);
        set_macro(&format!("type_{x}"), st, false);
        set_macro(&format!("polars_type_{x}"), pt, false);
        set_macro(&format!("string_length_{x}"), &sl.to_string(), false);
        set_macro(&format!("rename_{x}"), "", false);
        fields.push(DescribeFieldPayload {
            n: n.to_string(),
            s: st.to_string(),
            p: pt.to_string(),
            l: sl,
            r: String::new(),
        });
    }
    set_macro(
        "schema_payload",
        &serde_json::to_string(&DescribeSchemaPayload {
            v: SCHEMA_HANDOFF_PROTOCOL_VERSION,
            f: fields,
        })
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?,
        false,
    );
    Ok(s.len())
}

#[derive(Serialize)]
struct DescribeFieldPayload {
    n: String,
    s: String,
    p: String,
    l: usize,
    r: String,
}
#[derive(Serialize)]
struct DescribeSchemaPayload {
    v: u32,
    f: Vec<DescribeFieldPayload>,
}

pub fn file_summary(path: &str, det: bool, q: bool) -> ST_retcode {
    set_macro("cast_json", "", false);
    let r = match File::open(path).map(ParquetReader::new) {
        Ok(v) => v,
        Err(e) => {
            display(&format!("Error: {e}"));
            return 198;
        }
    };
    let df = match r.finish() {
        Ok(v) => v,
        Err(e) => {
            display(&format!("Error: {e:?}"));
            return 198;
        }
    };
    let mut lens = HashMap::new();
    if det {
        for (n, dt) in df.schema().iter() {
            if dt.is_string() {
                if let Ok(col) = df.column(n) {
                    if let Ok(ca) = col.str() {
                        lens.insert(
                            n.to_string(),
                            ca.into_iter()
                                .map(|v| v.map(|x| x.len()).unwrap_or(0))
                                .max()
                                .unwrap_or(0),
                        );
                    }
                }
            }
        }
    }
    match set_schema_macros(df.schema(), &lens, det, q) {
        Ok(c) => {
            set_macro("n_columns", &c.to_string(), false);
            set_macro("n_rows", &df.height().to_string(), false);
            0
        }
        Err(e) => {
            display(&format!("Error: {e:?}"));
            198
        }
    }
}

pub fn verify_parquet_path(path: &str) -> bool {
    let p = Path::new(path);
    if p.is_file() {
        return true;
    }
    if p.is_dir() {
        return WalkDir::new(p)
            .max_depth(3)
            .into_iter()
            .filter_map(Result::ok)
            .any(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
            });
    }
    if path.contains('*') || path.contains('?') || path.contains('[') {
        return glob(&if cfg!(windows) {
            path.replace('\\', "/")
        } else {
            path.to_string()
        })
        .map(|p| p.filter_map(Result::ok).next().is_some())
        .unwrap_or(false);
    }
    false
}

pub fn validate_stata_schema(infos: &[ExportField]) -> Result<(), Box<dyn Error>> {
    if count_stata_rows() == 0 {
        return Err("No rows".into());
    }
    for (i, info) in infos.iter().enumerate() {
        if is_stata_string_dtype(&info.dtype) {
            continue;
        }
        if let Some(val) = pull_numeric_cell(i + 1, 1) {
            if val.is_nan() && info.dtype != "float" && info.dtype != "double" {
                return Err(format!("NaN in {}", info.name).into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stata_to_polars_type_collapses_strings() {
        assert_eq!(stata_to_polars_type("strL"), DataType::String);
        assert_eq!(stata_to_polars_type("str20"), DataType::String);
        assert_eq!(stata_to_polars_type("anything"), DataType::String);
    }
    #[test]
    fn thread_pools_init() {
        warm_thread_pools();
        assert!(compute_pool_init_count() >= 1);
        assert!(io_pool_init_count() >= 1);
    }
}
