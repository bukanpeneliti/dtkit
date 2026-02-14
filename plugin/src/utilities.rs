pub const STATA_DATE_ORIGIN: i32 = 3653;
pub const STATA_EPOCH_MS: i64 = 315619200;
pub const TIME_MS: i64 = 1_000;
pub const TIME_US: i64 = 1_000_000;
pub const TIME_NS: i64 = 1_000_000_000;

use std::env;
use std::thread;

const ENV_DTPARQUET_THREADS: &str = "DTPARQUET_THREADS";
const ENV_POLARS_MAX_THREADS: &str = "POLARS_MAX_THREADS";

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

#[derive(Copy, Clone, Debug)]
pub enum BatchMode {
    ByRow,
    ByColumn,
}

fn determine_parallelization_strategy_impl(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize,
) -> BatchMode {
    if n_columns > available_cores * 2 && n_rows < 100_000 {
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
