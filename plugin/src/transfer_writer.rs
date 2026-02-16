use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, serde::Deserialize)]
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

pub fn validate_stata_schema(infos: &[ExportField]) -> Result<(), Box<dyn std::error::Error>> {
    use crate::stata_interface::count_rows;
    let total_rows = count_rows();
    if total_rows == 0 {
        return Err("No rows in Stata data to export".into());
    }

    for info in infos {
        let col_idx = info.name.parse::<usize>().unwrap_or(0);
        if col_idx == 0 {
            continue;
        }

        if is_stata_string_dtype(&info.dtype) {
            continue;
        }

        if let Some(val) = pull_numeric_cell(col_idx, 1) {
            if val.is_nan() && info.dtype != "float" && info.dtype != "double" {
                return Err(format!(
                    "Column '{}' has NaN values but Stata type '{}' cannot store them",
                    info.name, info.dtype
                )
                .into());
            }
        }
    }

    Ok(())
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

#[derive(Default)]
struct WriteQueueTelemetry {
    queue_peak: AtomicUsize,
    backpressure_events: AtomicUsize,
    queue_wait_ms: AtomicUsize,
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
    queue_telemetry: Arc<WriteQueueTelemetry>,
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
            let dtype = crate::mapping::export_field_polars_dtype(&info.dtype, &info.format);
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
        let queue_telemetry = Arc::new(WriteQueueTelemetry::default());

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
            queue_telemetry,
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
        self.queue_telemetry.queue_peak.load(Ordering::Relaxed)
    }

    pub fn queue_backpressure_events(&self) -> usize {
        self.queue_telemetry
            .backpressure_events
            .load(Ordering::Relaxed)
    }

    pub fn queue_wait_ms(&self) -> usize {
        self.queue_telemetry.queue_wait_ms.load(Ordering::Relaxed)
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
