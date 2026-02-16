//! Global configuration and environment variable keys for dtparquet.

/// Protocol version for schema handoff between ADO and plugin.
pub const SCHEMA_HANDOFF_PROTOCOL_VERSION: u32 = 2;

/// Number of rows to sample for schema validation.
pub const SCHEMA_VALIDATION_SAMPLE_ROWS: usize = 100;

/// Default batch size for reading/writing if not specified.
pub const DEFAULT_BATCH_SIZE: usize = 50_000;

// Environment Variable Keys
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

// Default Values
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
