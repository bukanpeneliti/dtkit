pub const SCHEMA_HANDOFF_PROTOCOL_VERSION: u32 = 2;

pub const STATA_DATE_ORIGIN: i32 = 3653;
pub const STATA_EPOCH_MS: i64 = 315619200;
pub const TIME_MS: i64 = 1_000;
pub const TIME_US: i64 = 1_000_000;
pub const TIME_NS: i64 = 1_000_000_000;

pub const DEFAULT_MEMORY_BUDGET_MB: usize = 512;
pub const ROW_ESTIMATE_BYTES: usize = 64;
pub const MIN_BATCH_SIZE: usize = 1_000;
pub const MAX_BATCH_SIZE: usize = 100_000;

const DEFAULT_BATCH_MIN_ROWS: usize = 10_000;
const DEFAULT_BATCH_MAX_ROWS: usize = 250_000;
const DEFAULT_BATCH_TARGET_MS: usize = 200;

const DEFAULT_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 8;
const MAX_WRITE_PIPELINE_QUEUE_CAPACITY: usize = 32;
const DEFAULT_WRITE_PIPELINE_MIN_ROWS: usize = 20_000;

const ENV_BATCH_MIN_ROWS: &str = "DTPARQUET_BATCH_MIN_ROWS";
const ENV_BATCH_MAX_ROWS: &str = "DTPARQUET_BATCH_MAX_ROWS";
const ENV_BATCH_TARGET_MS: &str = "DTPARQUET_BATCH_TARGET_MS";
const ENV_WRITE_PIPELINE_QUEUE_CAPACITY: &str = "DTPARQUET_WRITE_PIPELINE_QUEUE_CAPACITY";
const ENV_WRITE_PIPELINE_MIN_ROWS: &str = "DTPARQUET_WRITE_PIPELINE_MIN_ROWS";

pub fn batch_min_rows() -> usize {
    std::env::var(ENV_BATCH_MIN_ROWS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_MIN_ROWS)
}

pub fn batch_max_rows() -> usize {
    std::env::var(ENV_BATCH_MAX_ROWS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_MAX_ROWS)
}

pub fn batch_target_ms() -> usize {
    std::env::var(ENV_BATCH_TARGET_MS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_TARGET_MS)
}

pub fn write_pipeline_queue_capacity() -> usize {
    std::env::var(ENV_WRITE_PIPELINE_QUEUE_CAPACITY)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WRITE_PIPELINE_QUEUE_CAPACITY)
        .clamp(1, MAX_WRITE_PIPELINE_QUEUE_CAPACITY)
}

pub fn write_pipeline_min_rows() -> usize {
    std::env::var(ENV_WRITE_PIPELINE_MIN_ROWS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WRITE_PIPELINE_MIN_ROWS)
}
