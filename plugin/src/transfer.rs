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
) -> PolarsResult<(usize, usize)> {
    let total_rows = df.height();
    let mut loaded_rows = 0usize;
    let mut n_batches = 0usize;
    let mut batch_offset = 0usize;

    while batch_offset < total_rows {
        let batch_length = (total_rows - batch_offset).min(batch_tuner.selected_batch_size());
        let batch_df = df.slice(batch_offset as i64, batch_length);
        if batch_df.height() == 0 {
            break;
        }

        let batch_started_at = Instant::now();
        process_batch_with_strategy(
            &batch_df,
            start_index_base + batch_offset,
            transfer_columns,
            strategy,
            stata_offset,
        )?;

        let batch_rows = batch_df.height();
        loaded_rows += batch_rows;
        *processed_batches += 1;
        n_batches += 1;
        batch_offset += batch_rows;
        batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
    }

    Ok((loaded_rows, n_batches))
}

fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
) -> PolarsResult<()> {
    let row_count = batch.height();
    let prepared_columns = prepare_transfer_columns(batch, transfer_columns)?;
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        return process_row_range(
            &prepared_columns,
            start_index,
            0,
            row_count,
            transfer_columns,
            stata_offset,
        );
    }

    pool.install(|| match strategy {
        BatchMode::ByRow => {
            let n_workers = rayon::current_num_threads().max(1);
            let chunk_size = std::cmp::max(8_192, row_count.div_ceil(n_workers * 2));
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
    })
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
    for (transfer_column, col) in transfer_columns.iter().zip(prepared_columns.iter()) {
        write_transfer_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        )?;
    }
    Ok(())
}

fn to_range_error(context: &str) -> PolarsError {
    PolarsError::ComputeError(format!("Invalid Stata bounds in {context}").into())
}

fn validate_write_transfer_range(
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    if start_row >= end_row {
        return Ok(());
    }
    let row_start = start_index + start_row + 1 + stata_offset;
    let row_end_exclusive = start_index + end_row + 1 + stata_offset;
    let col = transfer_column.stata_col_index + 1;
    validate_stata_range(
        row_start,
        row_end_exclusive,
        col,
        col + 1,
        "write_transfer_column_range",
    )
    .map_err(|_| to_range_error("write_transfer_column_range"))
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
            write_numeric_iter(ctx, ctx.col.bool()?.iter(), |v| if v { 1.0 } else { 0.0 })
        }
        DataType::Int8 => write_numeric_iter(ctx, ctx.col.i8()?.iter(), |v| v as f64),
        DataType::Int16 => write_numeric_iter(ctx, ctx.col.i16()?.iter(), |v| v as f64),
        DataType::Int32 => write_numeric_iter(ctx, ctx.col.i32()?.iter(), |v| v as f64),
        DataType::Int64 => write_numeric_iter(ctx, ctx.col.i64()?.iter(), |v| v as f64),
        DataType::UInt8 => write_numeric_iter(ctx, ctx.col.u8()?.iter(), |v| v as f64),
        DataType::UInt16 => write_numeric_iter(ctx, ctx.col.u16()?.iter(), |v| v as f64),
        DataType::UInt32 => write_numeric_iter(ctx, ctx.col.u32()?.iter(), |v| v as f64),
        DataType::UInt64 => write_numeric_iter(ctx, ctx.col.u64()?.iter(), |v| v as f64),
        DataType::Float32 => write_numeric_iter(ctx, ctx.col.f32()?.iter(), |v| v as f64),
        DataType::Float64 => write_numeric_iter(ctx, ctx.col.f64()?.iter(), |v| v),
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
    write_numeric_iter(ctx, ca.physical().iter(), |v| {
        (v + STATA_DATE_ORIGIN) as f64
    })
}

fn write_time_values(ctx: &TransferContext) -> PolarsResult<()> {
    let physical = ctx.col.to_physical_repr();
    let ca = physical.i64()?;
    write_numeric_iter(ctx, ca.iter(), |v| (v / TIME_US) as f64)
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
    write_numeric_iter(ctx, ca.physical().iter(), |v| {
        v as f64 / factor + sec_shift_scaled
    })
}

fn write_numeric_iter<T, I, F>(ctx: &TransferContext, iter: I, mapper: F) -> PolarsResult<()>
where
    I: Iterator<Item = Option<T>>,
    F: Fn(T) -> f64 + Copy,
{
    let mut write_calls = 0u64;
    for (local_idx, value) in iter
        .skip(ctx.start_row)
        .take(ctx.end_row.saturating_sub(ctx.start_row))
        .enumerate()
    {
        let global_row_idx = ctx.start_row + local_idx + ctx.start_index;
        replace_number_unchecked(
            value.map(mapper),
            global_row_idx + 1 + ctx.stata_offset,
            ctx.transfer_column.stata_col_index + 1,
        );
        write_calls += 1;
    }
    add_transfer_metric_counts(write_calls, 0, 0, 0, 0);
    Ok(())
}

fn write_all_missing_range(
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    replacer: impl Fn(usize, usize),
) {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        replacer(
            global_row_idx + 1 + stata_offset,
            transfer_column.stata_col_index + 1,
        );
    }
}

fn write_string_values(ctx: &TransferContext) -> PolarsResult<()> {
    let mut write_calls = 0u64;
    let str_col = ctx.col.str()?;
    for (local_idx, value) in str_col
        .iter()
        .skip(ctx.start_row)
        .take(ctx.end_row.saturating_sub(ctx.start_row))
        .enumerate()
    {
        let global_row_idx = ctx.start_row + local_idx + ctx.start_index;
        let row = global_row_idx + 1 + ctx.stata_offset;
        let col = ctx.transfer_column.stata_col_index + 1;
        replace_string_ref_unchecked(value, row, col);
        write_calls += 1;
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
    write_all_missing_range(
        ctx.transfer_column,
        ctx.start_index,
        ctx.start_row,
        ctx.end_row,
        ctx.stata_offset,
        |row, col| {
            if as_string {
                replace_string_unchecked(None, row, col);
            } else {
                replace_number_unchecked(None, row, col);
            }
        },
    );
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
    validate_write_transfer_range(
        transfer_column,
        start_index,
        start_row,
        end_row,
        stata_offset,
    )?;
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
        series_from_stata_numeric_column_unchecked,
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
        let mut values = Vec::with_capacity(n_rows);
        for row_idx in 0..n_rows {
            values.push(pull_string_cell_with_buffer_unchecked(
                stata_col_index,
                offset + row_idx + 1,
                &mut str_buffer,
            ));
        }
        add_transfer_metric_counts(0, 0, 0, n_rows as u64, 0);
        return Ok(Series::new((&info.name).into(), values));
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

fn series_from_stata_numeric_column_unchecked(
    stata_col_index: usize,
    info: &ExportField,
    offset: usize,
    n_rows: usize,
) -> Result<Series, PolarsError> {
    let s = series_from_stata_numeric_dtype_unchecked(stata_col_index, info, offset, n_rows, true);
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
