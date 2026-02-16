use polars::datatypes::{AnyValue, TimeUnit};
use polars::prelude::*;
use rayon::prelude::*;
use std::time::Instant;

use crate::mapping::{is_stata_date_format, is_stata_datetime_format, is_stata_string_dtype};
use crate::mapping::{transfer_writer_kind_from_stata_type, FieldSpec, TransferWriterKind};
use crate::stata_interface::{
    pull_numeric_cell, pull_string_cell_with_buffer, pull_strl_cell_with_arena,
    record_transfer_conversion_failure, replace_number, replace_string, StrlArena,
};
use crate::utilities::{
    get_compute_thread_pool, AdaptiveBatchTuner, BatchMode, STATA_DATE_ORIGIN, STATA_EPOCH_MS,
    TIME_MS, TIME_NS, TIME_US,
};

#[path = "transfer_reader.rs"]
pub mod reader;

#[path = "transfer_writer.rs"]
pub mod writer;

pub use reader::TransferColumnSpec;
pub use writer::ExportField;
