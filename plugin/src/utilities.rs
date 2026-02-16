pub const STATA_DATE_ORIGIN: i32 = 3653;
pub const STATA_EPOCH_MS: i64 = 315619200;
pub const TIME_MS: i64 = 1_000;
pub const TIME_US: i64 = 1_000_000;
pub const TIME_NS: i64 = 1_000_000_000;

use rayon::{ThreadPool, ThreadPoolBuilder};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::thread;

const ENV_DTPARQUET_THREADS: &str = "DTPARQUET_THREADS";
const ENV_POLARS_MAX_THREADS: &str = "POLARS_MAX_THREADS";
const ENV_BATCH_AUTOTUNE_MODE: &str = "DTPARQUET_BATCH_AUTOTUNE_MODE";
const ENV_BATCH_MEMORY_MB: &str = "DTPARQUET_BATCH_MEMORY_MB";
const ENV_BATCH_MIN_ROWS: &str = "DTPARQUET_BATCH_MIN_ROWS";
const ENV_BATCH_MAX_ROWS: &str = "DTPARQUET_BATCH_MAX_ROWS";
const ENV_BATCH_TARGET_MS: &str = "DTPARQUET_BATCH_TARGET_MS";
const ENV_WRITE_PIPELINE_MODE: &str = "DTPARQUET_WRITE_PIPELINE_MODE";
const ENV_WRITE_PIPELINE_QUEUE_CAPACITY: &str = "DTPARQUET_WRITE_PIPELINE_QUEUE_CAPACITY";
const ENV_WRITE_PIPELINE_MIN_ROWS: &str = "DTPARQUET_WRITE_PIPELINE_MIN_ROWS";

static IO_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
static COMPUTE_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
static IO_POOL_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static COMPUTE_POOL_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

fn parse_env_usize(var: &str) -> Option<usize> {
    env::var(var).ok().and_then(|s| {
        let parsed = s.parse::<usize>().ok()?;
        if parsed > 0 {
            Some(parsed)
        } else {
            None
        }
    })
}

fn get_dtparquet_threads() -> Option<usize> {
    parse_env_usize(ENV_DTPARQUET_THREADS)
}

fn get_polars_threads() -> Option<usize> {
    parse_env_usize(ENV_POLARS_MAX_THREADS)
}

fn get_hardware_threads() -> usize {
    thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

fn resolve_thread_count_impl() -> usize {
    get_dtparquet_threads()
        .or_else(get_polars_threads)
        .unwrap_or_else(get_hardware_threads)
}

pub fn get_thread_count() -> usize {
    resolve_thread_count_impl()
}

pub fn get_io_thread_count() -> usize {
    let cores = get_hardware_threads();
    cores.clamp(2, 8)
}

pub fn get_compute_thread_count() -> usize {
    get_thread_count()
}

fn build_named_pool(
    threads: usize,
    pool_kind: &'static str,
) -> Result<ThreadPool, rayon::ThreadPoolBuildError> {
    ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(move |i| format!("dtparquet-{}-{}", pool_kind, i))
        .build()
}

fn create_io_thread_pool() -> ThreadPool {
    let threads = get_io_thread_count();
    build_named_pool(threads, "io")
        .or_else(|_| build_named_pool(2, "io"))
        .or_else(|_| build_named_pool(1, "io"))
        .unwrap()
}

fn create_compute_thread_pool() -> ThreadPool {
    let threads = get_compute_thread_count();
    build_named_pool(threads, "cpu")
        .or_else(|_| build_named_pool(1, "cpu"))
        .unwrap()
}

pub fn get_io_thread_pool() -> &'static ThreadPool {
    IO_THREAD_POOL.get_or_init(|| {
        IO_POOL_INIT_COUNT.fetch_add(1, Ordering::Relaxed);
        create_io_thread_pool()
    })
}

pub fn get_compute_thread_pool() -> &'static ThreadPool {
    COMPUTE_THREAD_POOL.get_or_init(|| {
        COMPUTE_POOL_INIT_COUNT.fetch_add(1, Ordering::Relaxed);
        create_compute_thread_pool()
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
/// Batch processing mode for parallel data transfer.
///
/// - `ByRow`: Process row-wise (default for most cases)
/// - `ByColumn`: Process column-wise (optimized for wide tables with few rows)
pub enum BatchMode {
    ByRow,
    ByColumn,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// Write pipeline execution mode.
///
/// - `ProducerConsumer`: Multi-threaded with queue (default for large datasets)
/// - `LegacyDirect`: Single-threaded direct write (legacy compatibility)
pub enum WritePipelineMode {
    ProducerConsumer,
    LegacyDirect,
}

const DEFAULT_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 8;
const MAX_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 32;
const DEFAULT_WRITE_PIPELINE_MIN_ROWS: usize = 20_000;

pub fn write_pipeline_mode() -> WritePipelineMode {
    env::var(ENV_WRITE_PIPELINE_MODE)
        .map(|raw| {
            let mode = raw.trim().to_ascii_lowercase();
            if mode == "legacy" || mode == "direct" || mode == "off" || mode == "fixed" {
                WritePipelineMode::LegacyDirect
            } else {
                WritePipelineMode::ProducerConsumer
            }
        })
        .unwrap_or(WritePipelineMode::LegacyDirect)
}

pub fn write_pipeline_queue_capacity() -> usize {
    parse_env_usize(ENV_WRITE_PIPELINE_QUEUE_CAPACITY)
        .unwrap_or(DEFAULT_WRITE_PIPELINE_QUEUE_CAPACITY)
        .clamp(1, MAX_WRITE_PIPELINE_QUEUE_CAPACITY)
}

pub fn write_pipeline_min_rows() -> usize {
    parse_env_usize(ENV_WRITE_PIPELINE_MIN_ROWS).unwrap_or(DEFAULT_WRITE_PIPELINE_MIN_ROWS)
}

fn determine_parallelization_strategy_impl(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize,
) -> BatchMode {
    if n_columns > available_cores * 2 && n_rows < 50_000 {
        BatchMode::ByColumn
    } else {
        BatchMode::ByRow
    }
}

pub fn determine_parallelization_strategy(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize,
) -> BatchMode {
    determine_parallelization_strategy_impl(n_columns, n_rows, available_cores)
}

// --- Batch calculation helpers (internal use only) ---
// These are preparatory for future refactoring slices; they are not called yet.
#[allow(dead_code)]
fn calc_n_batches(total_rows: usize, batch_size: usize) -> usize {
    (total_rows as f64 / batch_size as f64).ceil() as usize
}
#[allow(dead_code)]
fn calc_batch_offset(batch_index: usize, batch_size: usize) -> usize {
    batch_index * batch_size
}
#[allow(dead_code)]
fn calc_batch_length(batch_index: usize, batch_size: usize, total_rows: usize) -> usize {
    if (batch_index + 1) * batch_size > total_rows {
        total_rows - batch_index * batch_size
    } else {
        batch_size
    }
}

const DEFAULT_MEMORY_BUDGET_MB: usize = 512;
const ROW_ESTIMATE_BYTES: usize = 64;
const MIN_BATCH_SIZE: usize = 1_000;
const MAX_BATCH_SIZE: usize = 100_000;

pub fn calculate_adaptive_batch_size(
    n_columns: usize,
    estimated_row_width: usize,
    memory_budget_mb: Option<usize>,
) -> usize {
    let budget = memory_budget_mb.unwrap_or(DEFAULT_MEMORY_BUDGET_MB).max(1);
    let memory_bytes = budget.saturating_mul(1024 * 1024);
    let row_size = estimated_row_width.max(ROW_ESTIMATE_BYTES * n_columns / 10);
    let calculated = memory_bytes / row_size.max(1);
    calculated.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE)
}

fn batch_autotune_is_enabled() -> bool {
    env::var(ENV_BATCH_AUTOTUNE_MODE)
        .map(|mode| {
            let normalized = mode.trim().to_ascii_lowercase();
            normalized != "fixed"
                && normalized != "off"
                && normalized != "static"
                && normalized != "legacy"
        })
        .unwrap_or(true)
}

fn round_batch_size(value: usize) -> usize {
    let unit = if value >= 50_000 {
        5_000
    } else if value >= 10_000 {
        1_000
    } else {
        100
    };
    ((value + unit / 2) / unit).max(1) * unit
}

const DEFAULT_BATCH_MIN_ROWS: usize = 10_000;
const DEFAULT_BATCH_MAX_ROWS: usize = 250_000;
const DEFAULT_BATCH_TARGET_MS: usize = 200;

#[derive(Clone, Debug)]
pub struct AdaptiveBatchTuner {
    min_batch_size: usize,
    max_batch_size: usize,
    row_width_bytes: usize,
    memory_guardrail_rows: usize,
    target_batch_ms: f64,
    selected_batch_size: usize,
    smoothed_rows_per_ms: f64,
    tuning_adjustments: usize,
    autotune_enabled: bool,
}

impl AdaptiveBatchTuner {
    pub fn new(
        row_width_bytes: usize,
        configured_batch_size: usize,
        default_batch_size: usize,
    ) -> Self {
        let row_width_bytes = row_width_bytes.max(1);
        let memory_budget_mb = parse_env_usize(ENV_BATCH_MEMORY_MB)
            .unwrap_or(DEFAULT_MEMORY_BUDGET_MB)
            .max(1);
        let memory_guardrail_rows =
            (memory_budget_mb.saturating_mul(1024 * 1024) / row_width_bytes).max(1);

        let env_max_rows = parse_env_usize(ENV_BATCH_MAX_ROWS)
            .unwrap_or(DEFAULT_BATCH_MAX_ROWS)
            .max(1);
        let configured_ceiling = if configured_batch_size == 0 {
            usize::MAX
        } else {
            configured_batch_size.max(1)
        };
        let max_batch_size = env_max_rows
            .min(memory_guardrail_rows)
            .min(configured_ceiling)
            .max(1);

        let env_min_rows = parse_env_usize(ENV_BATCH_MIN_ROWS)
            .unwrap_or(DEFAULT_BATCH_MIN_ROWS)
            .max(1);
        let min_batch_size = env_min_rows.min(max_batch_size).max(1);

        let adaptive_seed =
            calculate_adaptive_batch_size(1, row_width_bytes, Some(memory_budget_mb));
        let seed_batch_size = if configured_batch_size > 0 {
            configured_batch_size
        } else if default_batch_size > 0 {
            adaptive_seed.max(default_batch_size)
        } else {
            adaptive_seed
        }
        .clamp(min_batch_size, max_batch_size);

        let target_batch_ms = parse_env_usize(ENV_BATCH_TARGET_MS)
            .unwrap_or(DEFAULT_BATCH_TARGET_MS)
            .max(1) as f64;

        Self {
            min_batch_size,
            max_batch_size,
            row_width_bytes,
            memory_guardrail_rows,
            target_batch_ms,
            selected_batch_size: seed_batch_size,
            smoothed_rows_per_ms: 0.0,
            tuning_adjustments: 0,
            autotune_enabled: batch_autotune_is_enabled(),
        }
    }

    pub fn selected_batch_size(&self) -> usize {
        self.selected_batch_size
    }

    pub fn row_width_bytes(&self) -> usize {
        self.row_width_bytes
    }

    pub fn memory_guardrail_rows(&self) -> usize {
        self.memory_guardrail_rows
    }

    pub fn tuning_adjustments(&self) -> usize {
        self.tuning_adjustments
    }

    pub fn tuning_mode(&self) -> &'static str {
        if self.autotune_enabled {
            "adaptive"
        } else {
            "fixed"
        }
    }

    pub fn observe_batch(&mut self, rows: usize, elapsed_ms: u128) -> usize {
        if rows == 0 || !self.autotune_enabled {
            return self.selected_batch_size;
        }

        let elapsed = elapsed_ms.max(1) as f64;
        let rows_per_ms = rows as f64 / elapsed;
        if self.smoothed_rows_per_ms == 0.0 {
            self.smoothed_rows_per_ms = rows_per_ms;
        } else {
            self.smoothed_rows_per_ms = (self.smoothed_rows_per_ms * 0.7) + (rows_per_ms * 0.3);
        }

        let target_rows = (self.smoothed_rows_per_ms * self.target_batch_ms)
            .round()
            .max(1.0) as usize;

        let mut next_batch_size = (self.selected_batch_size.saturating_mul(3) + target_rows) / 4;
        if elapsed > self.target_batch_ms * 1.35 {
            next_batch_size = next_batch_size.saturating_mul(9) / 10;
        } else if elapsed < self.target_batch_ms * 0.65 {
            next_batch_size = next_batch_size.saturating_mul(11) / 10;
        }

        let rounded = round_batch_size(next_batch_size.max(1));
        let clamped = rounded.clamp(self.min_batch_size, self.max_batch_size);
        if clamped != self.selected_batch_size {
            self.tuning_adjustments += 1;
            self.selected_batch_size = clamped;
        }
        self.selected_batch_size
    }
}
