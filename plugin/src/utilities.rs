pub const DAY_SHIFT_SAS_STATA: i32 = 3653;
pub const SEC_SHIFT_SAS_STATA: i64 = 315619200;
pub const SEC_MILLISECOND: i64 = 1_000;
pub const SEC_MICROSECOND: i64 = 1_000_000;
pub const SEC_NANOSECOND: i64 = 1_000_000_000;

use std::env;
use std::thread;

pub fn get_thread_count() -> usize {
    match env::var("POLARS_MAX_THREADS") {
        Ok(threads_str) => match threads_str.parse::<usize>() {
            Ok(threads) => threads,
            Err(_) => thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        },
        Err(_) => thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1),
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ParallelizationStrategy {
    ByRow,
    ByColumn,
}

pub fn determine_parallelization_strategy(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize,
) -> ParallelizationStrategy {
    if n_columns > available_cores * 2 && n_rows < 100_000 {
        ParallelizationStrategy::ByColumn
    } else {
        ParallelizationStrategy::ByRow
    }
}
