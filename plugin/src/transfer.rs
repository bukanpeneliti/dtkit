use polars::datatypes::{DataType, Field, PlSmallStr, TimeUnit};
use polars::prelude::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::logic::*;

// --- Types & Enums ---

#[derive(Clone, Debug)]
pub struct TransferColumnSpec {
    pub name: String,
    pub stata_col_index: usize,
    pub stata_type: String,
    pub writer_kind: TransferWriterKind,
}

pub struct TransferContext<'a> {
    pub col: &'a Column,
    pub transfer_column: &'a TransferColumnSpec,
    pub start_index: usize,
    pub start_row: usize,
    pub end_row: usize,
    pub stata_offset: usize,
}

// --- Numeric Converters (Macro-Generated) ---

macro_rules! pull_numeric_col {
    ($Chunked:ty, $col:expr, $off:expr, $n:expr, $name:expr, $cast:expr) => {{
        let ca: $Chunked = (0..$n)
            .map(|r| pull_numeric_cell_unchecked($col, $off + r + 1).map($cast))
            .collect();
        let mut s = ca.into_series();
        s.rename($name);
        Ok(s)
    }};
}

// --- Helper Functions ---

pub fn build_transfer_columns(all_columns: &[FieldSpec]) -> Vec<TransferColumnSpec> {
    all_columns
        .iter()
        .map(|col| TransferColumnSpec {
            name: col.name.clone(),
            stata_col_index: col.index,
            stata_type: col.stata_type.clone(),
            writer_kind: transfer_writer_kind_from_stata_type(&col.stata_type),
        })
        .collect()
}

pub fn estimate_transfer_row_width_bytes(transfer_columns: &[TransferColumnSpec]) -> usize {
    transfer_columns
        .iter()
        .map(|col| match col.writer_kind {
            TransferWriterKind::Numeric => 8,
            TransferWriterKind::Date => 4,
            TransferWriterKind::Time => 8,
            TransferWriterKind::Datetime => 8,
            TransferWriterKind::String => 48,
            TransferWriterKind::Strl => 128,
        })
        .sum::<usize>()
        .max(1)
}

pub fn dtype_mismatch_error(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    expected_kind: &str,
) -> PolarsError {
    PolarsError::ComputeError(
        format!(
            "Type mismatch for column '{}' at Stata type '{}': expected {}, got {:?}",
            transfer_column.name,
            transfer_column.stata_type,
            expected_kind,
            col.dtype()
        )
        .into(),
    )
}

// --- Polars to Stata (Reader Path) ---

pub fn sink_dataframe_in_batches(
    df: &DataFrame,
    start_index_base: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
    batch_tuner: &mut AdaptiveBatchTuner,
    processed_batches: &mut usize,
) -> PolarsResult<(usize, usize, u128, u128)> {
    let total_rows = df.height();
    let mut loaded_rows = 0usize;
    let mut n_batches = 0usize;
    let mut batch_offset = 0usize;
    let mut sink_prepare_elapsed_us = 0u128;
    let mut sink_write_elapsed_us = 0u128;

    while batch_offset < total_rows {
        let batch_length = (total_rows - batch_offset).min(batch_tuner.selected_batch_size());
        let batch_df = df.slice(batch_offset as i64, batch_length);
        if batch_df.height() == 0 {
            break;
        }

        let batch_started_at = Instant::now();
        let (batch_prepare_us, batch_write_us) = process_batch_with_strategy(
            &batch_df,
            start_index_base + batch_offset,
            transfer_columns,
            strategy,
            stata_offset,
        )?;
        sink_prepare_elapsed_us += batch_prepare_us;
        sink_write_elapsed_us += batch_write_us;

        let batch_rows = batch_df.height();
        loaded_rows += batch_rows;
        *processed_batches += 1;
        n_batches += 1;
        batch_offset += batch_rows;
        batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
    }

    Ok((
        loaded_rows,
        n_batches,
        sink_prepare_elapsed_us,
        sink_write_elapsed_us,
    ))
}

fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
) -> PolarsResult<(u128, u128)> {
    let row_count = batch.height();
    let prepare_started = Instant::now();
    let prepared_columns = prepare_transfer_columns(batch, transfer_columns)?;
    let prepare_elapsed_us = prepare_started.elapsed().as_micros();
    let write_started = Instant::now();
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        process_row_range(
            &prepared_columns,
            start_index,
            0,
            row_count,
            transfer_columns,
            stata_offset,
        )?;
        return Ok((prepare_elapsed_us, write_started.elapsed().as_micros()));
    }

    pool.install(|| match strategy {
        BatchMode::ByRow => {
            let n_workers = read_sink_worker_target(
                rayon::current_num_threads().max(1),
                transfer_columns.len(),
                row_count,
            );
            let chunk_size = std::cmp::max(8_192, row_count.div_ceil(n_workers));
            let n_chunks = row_count.div_ceil(chunk_size);
            (0..n_chunks).into_par_iter().try_for_each(|chunk_idx| {
                let start_row = chunk_idx * chunk_size;
                let end_row = (start_row + chunk_size).min(row_count);
                if start_row >= end_row {
                    return Ok(());
                }
                process_row_range(
                    &prepared_columns,
                    start_index,
                    start_row,
                    end_row,
                    transfer_columns,
                    stata_offset,
                )
            })
        }
        BatchMode::ByColumn => transfer_columns
            .par_iter()
            .zip(prepared_columns.par_iter())
            .try_for_each(|(transfer_column, col)| {
                write_transfer_column_range(
                    col,
                    transfer_column,
                    start_index,
                    0,
                    row_count,
                    stata_offset,
                )
            }),
    })?;

    Ok((prepare_elapsed_us, write_started.elapsed().as_micros()))
}

fn prepare_transfer_columns(
    batch: &DataFrame,
    transfer_columns: &[TransferColumnSpec],
) -> PolarsResult<Vec<Column>> {
    let mut prepared = Vec::with_capacity(transfer_columns.len());
    for transfer_column in transfer_columns {
        let col = batch.column(&transfer_column.name)?;
        let needs_string_cast = matches!(
            transfer_column.writer_kind,
            TransferWriterKind::String | TransferWriterKind::Strl
        ) && !matches!(col.dtype(), DataType::String | DataType::Null);

        if needs_string_cast {
            prepared.push(col.cast(&DataType::String).map_err(|_| {
                record_transfer_conversion_failure();
                dtype_mismatch_error(col, transfer_column, "string")
            })?);
        } else {
            prepared.push(col.clone());
        }
    }
    Ok(prepared)
}

fn process_row_range(
    prepared_columns: &[Column],
    start_index: usize,
    start_row: usize,
    end_row: usize,
    transfer_columns: &[TransferColumnSpec],
    stata_offset: usize,
) -> PolarsResult<()> {
    let range_len = end_row.saturating_sub(start_row);
    if range_len == 0 {
        return Ok(());
    }

    for (transfer_column, col) in transfer_columns.iter().zip(prepared_columns.iter()) {
        let sliced_col = col.slice(start_row as i64, range_len);
        write_transfer_column_range(
            &sliced_col,
            transfer_column,
            start_index + start_row,
            0,
            range_len,
            stata_offset,
        )?;
    }
    Ok(())
}

fn to_range_error(context: &str) -> PolarsError {
    PolarsError::ComputeError(format!("Invalid Stata bounds in {context}").into())
}

fn validate_read_transfer_range(
    col: usize,
    offset: usize,
    n_rows: usize,
    context: &str,
) -> PolarsResult<()> {
    if n_rows == 0 {
        return Ok(());
    }
    let row_start = offset + 1;
    let row_end_exclusive = offset + n_rows + 1;
    validate_stata_range(row_start, row_end_exclusive, col, col + 1, context)
        .map_err(|_| to_range_error(context))
}

pub fn write_numeric_column_range(ctx: &TransferContext) -> PolarsResult<()> {
    match ctx.col.dtype() {
        DataType::Boolean => {
            let ca = ctx.col.bool()?;
            if ca.null_count() == 0 {
                write_bool_iter_no_null(ctx, ca.into_no_null_iter())
            } else {
                write_bool_iter(ctx, ca.iter())
            }
        }
        DataType::Int8 => {
            let ca = ctx.col.i8()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_i8_slice_no_null(ctx, values)
                } else {
                    write_i8_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_i8_iter(ctx, ca.iter())
            }
        }
        DataType::Int16 => {
            let ca = ctx.col.i16()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_i16_slice_no_null(ctx, values)
                } else {
                    write_i16_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_i16_iter(ctx, ca.iter())
            }
        }
        DataType::Int32 => {
            let ca = ctx.col.i32()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_i32_slice_no_null(ctx, values)
                } else {
                    write_i32_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_i32_iter(ctx, ca.iter())
            }
        }
        DataType::Int64 => {
            let ca = ctx.col.i64()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_i64_slice_no_null(ctx, values)
                } else {
                    write_i64_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_i64_iter(ctx, ca.iter())
            }
        }
        DataType::UInt8 => {
            let ca = ctx.col.u8()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_u8_slice_no_null(ctx, values)
                } else {
                    write_u8_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_u8_iter(ctx, ca.iter())
            }
        }
        DataType::UInt16 => {
            let ca = ctx.col.u16()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_u16_slice_no_null(ctx, values)
                } else {
                    write_u16_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_u16_iter(ctx, ca.iter())
            }
        }
        DataType::UInt32 => {
            let ca = ctx.col.u32()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_u32_slice_no_null(ctx, values)
                } else {
                    write_u32_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_u32_iter(ctx, ca.iter())
            }
        }
        DataType::UInt64 => {
            let ca = ctx.col.u64()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_u64_slice_no_null(ctx, values)
                } else {
                    write_u64_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_u64_iter(ctx, ca.iter())
            }
        }
        DataType::Float32 => {
            let ca = ctx.col.f32()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_f32_slice_no_null(ctx, values)
                } else {
                    write_f32_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_f32_iter(ctx, ca.iter())
            }
        }
        DataType::Float64 => {
            let ca = ctx.col.f64()?;
            if ca.null_count() == 0 {
                if let Ok(values) = ca.cont_slice() {
                    write_f64_slice_no_null(ctx, values)
                } else {
                    write_f64_iter_no_null(ctx, ca.into_no_null_iter())
                }
            } else {
                write_f64_iter(ctx, ca.iter())
            }
        }
        DataType::Date => write_date_values(ctx),
        DataType::Time => write_time_values(ctx),
        DataType::Datetime(_, _) => write_datetime_values(ctx),
        DataType::Null => write_missing_range(ctx, false),
        _ => {
            record_transfer_conversion_failure();
            Err(dtype_mismatch_error(
                ctx.col,
                ctx.transfer_column,
                "numeric/date/time/datetime",
            ))
        }
    }
}

pub fn write_string_column_range(ctx: &TransferContext) -> PolarsResult<()> {
    match ctx.col.dtype() {
        DataType::String => write_string_values(ctx),
        DataType::Null => write_missing_range(ctx, true),
        _ => {
            let casted = ctx.col.cast(&DataType::String).map_err(|_| {
                record_transfer_conversion_failure();
                dtype_mismatch_error(ctx.col, ctx.transfer_column, "string")
            })?;
            let casted_ctx = TransferContext {
                col: &casted,
                transfer_column: ctx.transfer_column,
                start_index: ctx.start_index,
                start_row: ctx.start_row,
                end_row: ctx.end_row,
                stata_offset: ctx.stata_offset,
            };
            write_string_values(&casted_ctx)
        }
    }
}

fn write_date_values(ctx: &TransferContext) -> PolarsResult<()> {
    let ca = ctx.col.date()?;
    let physical = ca.physical();
    if ca.null_count() == 0 {
        write_date_iter_no_null(ctx, physical.into_no_null_iter())
    } else {
        write_date_iter(ctx, physical.iter())
    }
}

fn write_time_values(ctx: &TransferContext) -> PolarsResult<()> {
    let physical = ctx.col.to_physical_repr();
    let ca = physical.i64()?;
    if ca.null_count() == 0 {
        write_time_iter_no_null(ctx, ca.into_no_null_iter())
    } else {
        write_time_iter(ctx, ca.iter())
    }
}

fn write_datetime_values(ctx: &TransferContext) -> PolarsResult<()> {
    let ca = ctx.col.datetime()?;
    let unit = ca.time_unit();
    let factor = match unit {
        TimeUnit::Nanoseconds => (TIME_NS / TIME_MS) as f64,
        TimeUnit::Microseconds => (TIME_US / TIME_MS) as f64,
        TimeUnit::Milliseconds => 1.0,
    };
    let sec_shift_scaled = (STATA_EPOCH_MS as f64) * (TIME_MS as f64);
    let physical = ca.physical();
    if ca.null_count() == 0 {
        write_datetime_iter_no_null(ctx, physical.into_no_null_iter(), factor, sec_shift_scaled)
    } else {
        write_datetime_iter(ctx, physical.iter(), factor, sec_shift_scaled)
    }
}

#[inline(always)]
fn write_bool_iter<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = Option<bool>>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        if let Some(v) = value {
            unsafe { vstore(col, row, if v { 1.0 } else { 0.0 }) };
        }
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_bool_iter_no_null<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = bool>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        unsafe { vstore(col, row, if value { 1.0 } else { 0.0 }) };
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_date_iter<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = Option<i32>>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        if let Some(v) = value {
            unsafe { vstore(col, row, (v + STATA_DATE_ORIGIN) as f64) };
        }
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_date_iter_no_null<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = i32>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        unsafe { vstore(col, row, (value + STATA_DATE_ORIGIN) as f64) };
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_time_iter<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = Option<i64>>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        if let Some(v) = value {
            unsafe { vstore(col, row, (v / TIME_US) as f64) };
        }
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_time_iter_no_null<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
where
    I: Iterator<Item = i64>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        unsafe { vstore(col, row, (value / TIME_US) as f64) };
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_datetime_iter<I>(
    ctx: &TransferContext,
    iter: I,
    factor: f64,
    sec_shift_scaled: f64,
) -> PolarsResult<()>
where
    I: Iterator<Item = Option<i64>>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        if let Some(v) = value {
            unsafe { vstore(col, row, v as f64 / factor + sec_shift_scaled) };
        }
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

#[inline(always)]
fn write_datetime_iter_no_null<I>(
    ctx: &TransferContext,
    iter: I,
    factor: f64,
    sec_shift_scaled: f64,
) -> PolarsResult<()>
where
    I: Iterator<Item = i64>,
{
    let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let vstore = stata_sys::vstore_unchecked_fn();
    for value in iter {
        unsafe { vstore(col, row, value as f64 / factor + sec_shift_scaled) };
        row += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

macro_rules! define_typed_numeric_writers_cast {
    ($iter_fn:ident, $iter_no_null_fn:ident, $slice_no_null_fn:ident, $ty:ty) => {
        #[inline(always)]
        fn $iter_fn<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
        where
            I: Iterator<Item = Option<$ty>>,
        {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for value in iter {
                if let Some(v) = value {
                    unsafe { vstore(col, row, v as f64) };
                }
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }

        #[inline(always)]
        fn $iter_no_null_fn<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
        where
            I: Iterator<Item = $ty>,
        {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for value in iter {
                unsafe { vstore(col, row, value as f64) };
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }

        #[inline(always)]
        fn $slice_no_null_fn(ctx: &TransferContext, values: &[$ty]) -> PolarsResult<()> {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for &value in values {
                unsafe { vstore(col, row, value as f64) };
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }
    };
}

macro_rules! define_typed_numeric_writers_identity {
    ($iter_fn:ident, $iter_no_null_fn:ident, $slice_no_null_fn:ident, $ty:ty) => {
        #[inline(always)]
        fn $iter_fn<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
        where
            I: Iterator<Item = Option<$ty>>,
        {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for value in iter {
                if let Some(v) = value {
                    unsafe { vstore(col, row, v) };
                }
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }

        #[inline(always)]
        fn $iter_no_null_fn<I>(ctx: &TransferContext, iter: I) -> PolarsResult<()>
        where
            I: Iterator<Item = $ty>,
        {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for value in iter {
                unsafe { vstore(col, row, value) };
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }

        #[inline(always)]
        fn $slice_no_null_fn(ctx: &TransferContext, values: &[$ty]) -> PolarsResult<()> {
            let write_calls = (ctx.end_row.saturating_sub(ctx.start_row)) as u64;
            let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
            let col = (ctx.transfer_column.stata_col_index + 1) as i32;
            let vstore = stata_sys::vstore_unchecked_fn();
            for &value in values {
                unsafe { vstore(col, row, value) };
                row += 1;
            }
            add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
            Ok(())
        }
    };
}

define_typed_numeric_writers_cast!(
    write_i8_iter,
    write_i8_iter_no_null,
    write_i8_slice_no_null,
    i8
);
define_typed_numeric_writers_cast!(
    write_i16_iter,
    write_i16_iter_no_null,
    write_i16_slice_no_null,
    i16
);
define_typed_numeric_writers_cast!(
    write_i32_iter,
    write_i32_iter_no_null,
    write_i32_slice_no_null,
    i32
);
define_typed_numeric_writers_cast!(
    write_i64_iter,
    write_i64_iter_no_null,
    write_i64_slice_no_null,
    i64
);
define_typed_numeric_writers_cast!(
    write_u8_iter,
    write_u8_iter_no_null,
    write_u8_slice_no_null,
    u8
);
define_typed_numeric_writers_cast!(
    write_u16_iter,
    write_u16_iter_no_null,
    write_u16_slice_no_null,
    u16
);
define_typed_numeric_writers_cast!(
    write_u32_iter,
    write_u32_iter_no_null,
    write_u32_slice_no_null,
    u32
);
define_typed_numeric_writers_cast!(
    write_u64_iter,
    write_u64_iter_no_null,
    write_u64_slice_no_null,
    u64
);
define_typed_numeric_writers_cast!(
    write_f32_iter,
    write_f32_iter_no_null,
    write_f32_slice_no_null,
    f32
);
define_typed_numeric_writers_identity!(
    write_f64_iter,
    write_f64_iter_no_null,
    write_f64_slice_no_null,
    f64
);

#[inline(always)]
fn write_string_values(ctx: &TransferContext) -> PolarsResult<()> {
    let mut write_calls = 0u64;
    let str_col = ctx.col.str()?;
    let mut row = (ctx.start_index + 1 + ctx.stata_offset) as i32;
    let col = (ctx.transfer_column.stata_col_index + 1) as i32;
    let sstore = stata_sys::sstore_unchecked_fn();
    let mut buffer: Vec<u8> = Vec::new();

    if str_col.null_count() == 0 {
        for s in str_col.into_no_null_iter() {
            if !s.is_empty() {
                buffer.clear();
                buffer.extend_from_slice(s.as_bytes());
                buffer.push(0);
                unsafe {
                    sstore(col, row, buffer.as_mut_ptr() as *mut std::os::raw::c_char);
                }
                write_calls += 1;
            }
            row += 1;
        }
    } else {
        for value in str_col.iter() {
            let Some(s) = value else {
                row += 1;
                continue;
            };
            if s.is_empty() {
                row += 1;
                continue;
            }
            buffer.clear();
            buffer.extend_from_slice(s.as_bytes());
            buffer.push(0);
            unsafe {
                sstore(col, row, buffer.as_mut_ptr() as *mut std::os::raw::c_char);
            }
            write_calls += 1;
            row += 1;
        }
    }
    add_transfer_metric_counts(0, write_calls, 0, 0, 0);
    Ok(())
}

fn write_strict_typed_numeric_column_range(
    ctx: &TransferContext,
    expected_name: &'static str,
    expected_dtype: fn(&DataType) -> bool,
    writer: fn(&TransferContext) -> PolarsResult<()>,
) -> PolarsResult<()> {
    if expected_dtype(ctx.col.dtype()) {
        return writer(ctx);
    }

    if matches!(ctx.col.dtype(), DataType::Null) {
        return write_missing_range(ctx, false);
    }

    record_transfer_conversion_failure();
    Err(dtype_mismatch_error(
        ctx.col,
        ctx.transfer_column,
        expected_name,
    ))
}

fn write_missing_range(ctx: &TransferContext, as_string: bool) -> PolarsResult<()> {
    let n_calls = (ctx.end_row - ctx.start_row) as u64;
    if as_string {
        add_transfer_metric_counts(0, n_calls, 0, 0, 0);
    } else {
        add_transfer_metric_counts(n_calls, 0, 0, 0, 0);
    }
    Ok(())
}

pub(crate) fn write_transfer_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    let ctx = TransferContext {
        col,
        transfer_column,
        start_index,
        start_row,
        end_row,
        stata_offset,
    };

    match transfer_column.writer_kind {
        TransferWriterKind::Numeric => write_numeric_column_range(&ctx),
        TransferWriterKind::Date => write_strict_typed_numeric_column_range(
            &ctx,
            "date",
            |dt| matches!(dt, DataType::Date),
            write_date_values,
        ),
        TransferWriterKind::Time => write_strict_typed_numeric_column_range(
            &ctx,
            "time",
            |dt| matches!(dt, DataType::Time),
            write_time_values,
        ),
        TransferWriterKind::Datetime => write_strict_typed_numeric_column_range(
            &ctx,
            "datetime",
            |dt| matches!(dt, DataType::Datetime(_, _)),
            write_datetime_values,
        ),
        TransferWriterKind::String | TransferWriterKind::Strl => write_string_column_range(&ctx),
    }
}

// --- Stata to Polars (Writer Path) ---

pub fn read_batch_from_columns(
    column_info: &[ExportField],
    offset: usize,
    n_rows: usize,
) -> PolarsResult<DataFrame> {
    read_batch_from_columns_with_loader(
        column_info,
        offset,
        n_rows,
        "read_batch_from_columns",
        series_from_stata_column_unchecked,
        true,
    )
}

pub fn read_batch_numeric_from_columns(
    column_info: &[ExportField],
    offset: usize,
    n_rows: usize,
) -> PolarsResult<DataFrame> {
    read_batch_from_columns_with_loader(
        column_info,
        offset,
        n_rows,
        "read_batch_numeric_from_columns",
        |coli, info, off, nr| {
            let s = series_from_stata_numeric_dtype_unchecked(coli, info, off, nr, true);
            if s.is_ok() {
                add_transfer_metric_counts(0, 0, nr as u64, 0, 0);
            }
            s
        },
        false,
    )
}

fn read_batch_from_columns_with_loader(
    column_info: &[ExportField],
    offset: usize,
    n_rows: usize,
    context_name: &'static str,
    loader: fn(usize, &ExportField, usize, usize) -> Result<Series, PolarsError>,
    emit_parallel_macro: bool,
) -> PolarsResult<DataFrame> {
    if !column_info.is_empty() {
        let row_start = offset + 1;
        let row_end_exclusive = offset + n_rows + 1;
        validate_stata_range(
            row_start,
            row_end_exclusive,
            1,
            column_info.len() + 1,
            context_name,
        )
        .map_err(|_| to_range_error(context_name))?;
    }

    let pool = get_compute_thread_pool();
    let use_parallel = pool.current_num_threads() > 1 && column_info.len() >= 6 && n_rows >= 20_000;

    let columns = if use_parallel {
        let mut indexed: Vec<(usize, Column)> = pool.install(|| {
            column_info
                .par_iter()
                .enumerate()
                .map(|(idx, info)| {
                    loader(idx + 1, info, offset, n_rows).map(|s| (idx, s.into_column()))
                })
                .collect::<PolarsResult<Vec<(usize, Column)>>>()
        })?;
        indexed.sort_by_key(|(idx, _)| *idx);
        indexed.into_iter().map(|(_, c)| c).collect()
    } else {
        let mut cols = Vec::with_capacity(column_info.len());
        for (idx, info) in column_info.iter().enumerate() {
            cols.push(loader(idx + 1, info, offset, n_rows)?.into_column());
        }
        cols
    };

    if emit_parallel_macro {
        if use_parallel {
            set_macro("write_collect_parallel", "1", true);
        } else {
            set_macro("write_collect_parallel", "0", true);
        }
    }

    DataFrame::new_infer_height(columns)
}

pub fn series_from_stata_column(
    stata_col_index: usize,
    info: &ExportField,
    offset: usize,
    n_rows: usize,
) -> Result<Series, PolarsError> {
    validate_read_transfer_range(stata_col_index, offset, n_rows, "series_from_stata_column")?;
    series_from_stata_column_unchecked(stata_col_index, info, offset, n_rows)
}

fn series_from_stata_column_unchecked(
    stata_col_index: usize,
    info: &ExportField,
    offset: usize,
    n_rows: usize,
) -> Result<Series, PolarsError> {
    if info.dtype == "strl" {
        let mut strl_arena = StrlArena::new();
        let mut values = Vec::with_capacity(n_rows);
        for row_idx in 0..n_rows {
            values.push(pull_strl_cell_with_arena_unchecked(
                stata_col_index,
                offset + row_idx + 1,
                &mut strl_arena,
            ));
        }
        add_transfer_metric_counts(0, 0, 0, 0, n_rows as u64);
        return Ok(Series::new((&info.name).into(), values));
    }

    if is_stata_string_dtype(&info.dtype) {
        let width = info.str_length.max(1);
        let mut str_buffer: Vec<i8> = vec![0; width.saturating_add(1)];
        let mut builder = StringChunkedBuilder::new(PlSmallStr::from(&info.name), n_rows);

        for row_idx in 0..n_rows {
            let s = pull_string_cell_as_str_unchecked(
                stata_col_index,
                offset + row_idx + 1,
                &mut str_buffer,
            );
            builder.append_option(s);
        }
        add_transfer_metric_counts(0, 0, 0, n_rows as u64, 0);
        return Ok(builder.finish().into_series());
    }

    if is_stata_date_format(&info.format) {
        let values: Int32Chunked = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell_unchecked(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i32 - STATA_DATE_ORIGIN)
            })
            .collect();
        add_transfer_metric_counts(0, 0, n_rows as u64, 0, 0);
        let mut s = values.into_series();
        s.rename((&info.name).into());
        return s.cast(&DataType::Date);
    }

    if is_stata_datetime_format(&info.format) {
        let values: Int64Chunked = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell_unchecked(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i64 - ((STATA_EPOCH_MS as f64) * (TIME_MS as f64)) as i64)
            })
            .collect();
        add_transfer_metric_counts(0, 0, n_rows as u64, 0, 0);
        let mut s = values.into_series();
        s.rename((&info.name).into());
        return s.cast(&DataType::Datetime(TimeUnit::Milliseconds, None));
    }

    let s = series_from_stata_numeric_dtype_unchecked(stata_col_index, info, offset, n_rows, false);
    if s.is_ok() {
        add_transfer_metric_counts(0, 0, n_rows as u64, 0, 0);
    }
    s
}

fn series_from_stata_numeric_dtype_unchecked(
    stata_col_index: usize,
    info: &ExportField,
    offset: usize,
    n_rows: usize,
    strict_numeric: bool,
) -> Result<Series, PolarsError> {
    match info.dtype.as_str() {
        "byte" => pull_numeric_col!(
            Int8Chunked,
            stata_col_index,
            offset,
            n_rows,
            (&info.name).into(),
            |v| v as i8
        ),
        "int" => pull_numeric_col!(
            Int16Chunked,
            stata_col_index,
            offset,
            n_rows,
            (&info.name).into(),
            |v| v as i16
        ),
        "long" => pull_numeric_col!(
            Int32Chunked,
            stata_col_index,
            offset,
            n_rows,
            (&info.name).into(),
            |v| v as i32
        ),
        "float" => pull_numeric_col!(
            Float32Chunked,
            stata_col_index,
            offset,
            n_rows,
            (&info.name).into(),
            |v| v as f32
        ),
        dtype if dtype == "double" || !strict_numeric => pull_numeric_col!(
            Float64Chunked,
            stata_col_index,
            offset,
            n_rows,
            (&info.name).into(),
            |v| v
        ),
        _ => Err(PolarsError::ComputeError(
            format!("Non-numeric field '{}' in numeric fast path", info.name).into(),
        )),
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

        let batch_size_hint = Arc::new(AtomicUsize::new(safe_batch_size));

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

    pub fn join_pipeline_worker(&self) {}

    fn read_batch(&self, batch_offset: usize, batch_rows: usize) -> PolarsResult<DataFrame> {
        read_batch_from_columns(&self.column_info, self.start_row + batch_offset, batch_rows)
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
        let df = self.read_and_observe_batch(self.n_rows)?;
        Ok(df)
    }
}

impl StataRowSource {
    fn read_and_observe_batch(&self, requested_size: usize) -> PolarsResult<DataFrame> {
        let offset = self
            .current_offset
            .fetch_add(requested_size, Ordering::Relaxed);
        if offset >= self.n_rows {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }
        let read_count = std::cmp::min(requested_size, self.n_rows - offset);

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
