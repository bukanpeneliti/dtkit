use polars::datatypes::{AnyValue, DataType, Field, PlSmallStr, TimeUnit};
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

#[derive(Copy, Clone, Debug)]
pub enum CellConversion<T> {
    Value(Option<T>),
    Mismatch,
}

// --- Numeric Converters (Macro-Generated) ---

macro_rules! generate_numeric_converter {
    ($name:ident, $variant:ident, $type:ty) => {
        pub fn $name(value: AnyValue<'_>) -> CellConversion<f64> {
            match value {
                AnyValue::$variant(v) => CellConversion::Value(Some(v as f64)),
                AnyValue::Null => CellConversion::Value(None),
                _ => CellConversion::Mismatch,
            }
        }
    };
}

generate_numeric_converter!(convert_i8_to_f64, Int8, i8);
generate_numeric_converter!(convert_i16_to_f64, Int16, i16);
generate_numeric_converter!(convert_i32_to_f64, Int32, i32);
generate_numeric_converter!(convert_i64_to_f64, Int64, i64);
generate_numeric_converter!(convert_u8_to_f64, UInt8, u8);
generate_numeric_converter!(convert_u16_to_f64, UInt16, u16);
generate_numeric_converter!(convert_u32_to_f64, UInt32, u32);
generate_numeric_converter!(convert_u64_to_f64, UInt64, u64);
generate_numeric_converter!(convert_f32_to_f64, Float32, f32);

pub fn convert_boolean_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Boolean(v) => CellConversion::Value(Some(if v { 1.0 } else { 0.0 })),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_f64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Float64(v) => CellConversion::Value(Some(v)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_date_to_stata_days(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Date(v) => CellConversion::Value(Some((v + STATA_DATE_ORIGIN) as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_time_to_stata_millis(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Time(v) => CellConversion::Value(Some((v / TIME_US) as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

fn datetime_unit_factor(unit: TimeUnit) -> f64 {
    match unit {
        TimeUnit::Nanoseconds => (TIME_NS / TIME_MS) as f64,
        TimeUnit::Microseconds => (TIME_US / TIME_MS) as f64,
        TimeUnit::Milliseconds => 1.0,
    }
}

fn datetime_to_stata_clock(value: i64, unit: TimeUnit) -> f64 {
    let sec_shift_scaled = (STATA_EPOCH_MS as f64) * (TIME_MS as f64);
    value as f64 / datetime_unit_factor(unit) + sec_shift_scaled
}

pub fn convert_datetime_to_stata_clock(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Datetime(v, unit, _) => {
            CellConversion::Value(Some(datetime_to_stata_clock(v, unit)))
        }
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_strict_string(value: AnyValue<'_>) -> CellConversion<String> {
    match value {
        AnyValue::String(v) => CellConversion::Value(Some(v.to_string())),
        AnyValue::StringOwned(v) => CellConversion::Value(Some(v.to_string())),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
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
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        return process_row_range(
            batch,
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
            let chunk_size = std::cmp::max(512, row_count.div_ceil(n_workers * 8));
            (0..row_count)
                .into_par_iter()
                .chunks(chunk_size)
                .try_for_each(|chunk| {
                    let start_row = chunk[0];
                    let end_row = chunk[chunk.len() - 1] + 1;
                    process_row_range(
                        batch,
                        start_index,
                        start_row,
                        end_row,
                        transfer_columns,
                        stata_offset,
                    )
                })
        }
        BatchMode::ByColumn => transfer_columns.par_iter().try_for_each(|transfer_column| {
            let col = batch.column(&transfer_column.name)?;
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

fn process_row_range(
    batch: &DataFrame,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    transfer_columns: &[TransferColumnSpec],
    stata_offset: usize,
) -> PolarsResult<()> {
    for transfer_column in transfer_columns {
        let col = batch.column(&transfer_column.name)?;
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

pub fn write_numeric_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    let converter = match col.dtype() {
        DataType::Boolean => convert_boolean_to_f64,
        DataType::Int8 => convert_i8_to_f64,
        DataType::Int16 => convert_i16_to_f64,
        DataType::Int32 => convert_i32_to_f64,
        DataType::Int64 => convert_i64_to_f64,
        DataType::UInt8 => convert_u8_to_f64,
        DataType::UInt16 => convert_u16_to_f64,
        DataType::UInt32 => convert_u32_to_f64,
        DataType::UInt64 => convert_u64_to_f64,
        DataType::Float32 => convert_f32_to_f64,
        DataType::Float64 => convert_f64_to_f64,
        DataType::Date => convert_date_to_stata_days,
        DataType::Time => convert_time_to_stata_millis,
        DataType::Datetime(_, _) => convert_datetime_to_stata_clock,
        DataType::Null => {
            write_all_missing_numeric_range(
                transfer_column,
                start_index,
                start_row,
                end_row,
                stata_offset,
            );
            return Ok(());
        }
        _ => {
            record_transfer_conversion_failure();
            return Err(dtype_mismatch_error(
                col,
                transfer_column,
                "numeric/date/time/datetime",
            ));
        }
    };

    write_numeric_with_converter(
        col,
        transfer_column,
        start_index,
        start_row,
        end_row,
        stata_offset,
        converter,
    )
}

pub fn write_string_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::String => write_string_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_strict_string,
        ),
        DataType::Null => {
            write_all_missing_string_range(
                transfer_column,
                start_index,
                start_row,
                end_row,
                stata_offset,
            );
            Ok(())
        }
        _ => {
            let casted = col.cast(&DataType::String).map_err(|_| {
                record_transfer_conversion_failure();
                dtype_mismatch_error(col, transfer_column, "string")
            })?;
            write_string_with_converter(
                &casted,
                transfer_column,
                start_index,
                start_row,
                end_row,
                stata_offset,
                convert_strict_string,
            )
        }
    }
}

fn write_numeric_with_converter(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    converter: fn(AnyValue<'_>) -> CellConversion<f64>,
) -> PolarsResult<()> {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        let value = col.get(row_idx)?;
        match converter(value) {
            CellConversion::Value(number) => {
                replace_number(
                    number,
                    global_row_idx + 1 + stata_offset,
                    transfer_column.stata_col_index + 1,
                );
            }
            CellConversion::Mismatch => {
                record_transfer_conversion_failure();
                return Err(dtype_mismatch_error(
                    col,
                    transfer_column,
                    "numeric/date/time/datetime",
                ));
            }
        }
    }
    Ok(())
}

fn write_all_missing_numeric_range(
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        replace_number(
            None,
            global_row_idx + 1 + stata_offset,
            transfer_column.stata_col_index + 1,
        );
    }
}

fn write_string_with_converter(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    converter: fn(AnyValue<'_>) -> CellConversion<String>,
) -> PolarsResult<()> {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        let value = col.get(row_idx)?;
        match converter(value) {
            CellConversion::Value(text) => {
                replace_string(
                    text,
                    global_row_idx + 1 + stata_offset,
                    transfer_column.stata_col_index + 1,
                );
            }
            CellConversion::Mismatch => {
                record_transfer_conversion_failure();
                return Err(dtype_mismatch_error(col, transfer_column, "string"));
            }
        }
    }
    Ok(())
}

fn write_all_missing_string_range(
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) {
    for row_idx in start_row..end_row {
        let global_row_idx = row_idx + start_index;
        replace_string(
            None,
            global_row_idx + 1 + stata_offset,
            transfer_column.stata_col_index + 1,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn write_strict_typed_numeric_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
    expected_name: &'static str,
    expected_dtype: fn(&DataType) -> bool,
    converter: fn(AnyValue<'_>) -> CellConversion<f64>,
) -> PolarsResult<()> {
    if expected_dtype(col.dtype()) {
        return write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            converter,
        );
    }

    if matches!(col.dtype(), DataType::Null) {
        write_all_missing_numeric_range(
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        );
        return Ok(());
    }

    record_transfer_conversion_failure();
    Err(dtype_mismatch_error(col, transfer_column, expected_name))
}

pub(crate) fn write_transfer_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match transfer_column.writer_kind {
        TransferWriterKind::Numeric => write_numeric_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
        TransferWriterKind::Date => write_strict_typed_numeric_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            "date",
            |dt| matches!(dt, DataType::Date),
            convert_date_to_stata_days,
        ),
        TransferWriterKind::Time => write_strict_typed_numeric_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            "time",
            |dt| matches!(dt, DataType::Time),
            convert_time_to_stata_millis,
        ),
        TransferWriterKind::Datetime => write_strict_typed_numeric_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            "datetime",
            |dt| matches!(dt, DataType::Datetime(_, _)),
            convert_datetime_to_stata_clock,
        ),
        TransferWriterKind::String | TransferWriterKind::Strl => write_string_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        ),
    }
}

// --- Stata to Polars (Writer Path) ---

pub fn read_batch_from_columns(
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

pub fn series_from_stata_column(
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

    if is_stata_string_dtype(&info.dtype) {
        let width = info.str_length.max(1);
        let mut str_buffer: Vec<i8> = vec![0; width.saturating_add(1)];
        let values: Vec<String> = (0..n_rows)
            .map(|row_idx| {
                pull_string_cell_with_buffer(stata_col_index, offset + row_idx + 1, &mut str_buffer)
            })
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if is_stata_date_format(&info.format) {
        let values: Vec<Option<i32>> = (0..n_rows)
            .map(|row_idx| {
                pull_numeric_cell(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i32 - STATA_DATE_ORIGIN)
            })
            .collect();
        return Series::new((&info.name).into(), values).cast(&DataType::Date);
    }

    if is_stata_datetime_format(&info.format) {
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
    queue_capacity: usize,
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
            queue_capacity: 0,
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
        "legacy_direct"
    }

    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    pub fn queue_peak(&self) -> usize {
        0
    }
    pub fn queue_backpressure_events(&self) -> usize {
        0
    }
    pub fn queue_wait_ms(&self) -> usize {
        0
    }
    pub fn queue_produced_batches(&self) -> usize {
        self.processed_batches()
    }
    pub fn queue_consumed_batches(&self) -> usize {
        self.processed_batches()
    }

    pub fn join_pipeline_worker(&self) {
        let _ = self;
    }

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

    fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
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
}
