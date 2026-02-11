use polars::prelude::*;
use polars_sql::SQLContext;
use std::collections::HashMap;
use std::error::Error;
use std::fs::create_dir_all;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::metadata::{extract_dtmeta, DTMETA_KEY};
use crate::sql_from_if::stata_to_sql;
use crate::stata_interface::{get_macro, n_obs, read_numeric, read_string, read_string_strl};
use crate::utilities::{DAY_SHIFT_SAS_STATA, SEC_MILLISECOND, SEC_SHIFT_SAS_STATA};

#[derive(Clone, Debug)]
pub struct StataColumnInfo {
    pub name: String,
    pub dtype: String,
    pub format: String,
    pub str_length: usize,
}

pub struct StataDataScan {
    column_info: Vec<StataColumnInfo>,
    start_row: usize,
    n_rows: usize,
    batch_size: usize,
    current_offset: AtomicUsize,
    schema: Arc<Schema>,
    stata_api_lock: Mutex<()>,
}

impl StataDataScan {
    pub fn new(
        column_info: Vec<StataColumnInfo>,
        start_row: usize,
        n_rows: usize,
        batch_size: usize,
    ) -> Self {
        let mut fields = Vec::with_capacity(column_info.len());
        for info in &column_info {
            let dtype = match info.dtype.as_str() {
                "byte" => DataType::Int8,
                "int" => DataType::Int16,
                "long" => DataType::Int32,
                "float" => DataType::Float32,
                "double" => DataType::Float64,
                _ if info.dtype == "strl" || info.dtype.starts_with("str") => DataType::String,
                _ => DataType::Float64,
            };
            let dtype = if info.format.starts_with("%td") {
                DataType::Date
            } else if info.format.starts_with("%tc") {
                DataType::Datetime(TimeUnit::Milliseconds, None)
            } else {
                dtype
            };
            fields.push(Field::new(PlSmallStr::from(&info.name), dtype));
        }

        StataDataScan {
            column_info,
            start_row,
            n_rows,
            batch_size,
            current_offset: AtomicUsize::new(0),
            schema: Arc::new(Schema::from_iter(fields)),
            stata_api_lock: Mutex::new(()),
        }
    }
}

impl AnonymousScan for StataDataScan {
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

        let _lock = self.stata_api_lock.lock().unwrap();
        let df = self.read_batch(offset, read_count)?;
        Ok(df)
    }

    fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        let offset = self
            .current_offset
            .fetch_add(self.batch_size, Ordering::Relaxed);
        if offset >= self.n_rows {
            return Ok(None);
        }
        let read_count = std::cmp::min(self.batch_size, self.n_rows - offset);

        let _lock = self.stata_api_lock.lock().unwrap();
        let df = self.read_batch(offset, read_count)?;
        Ok(Some(df))
    }
}

impl StataDataScan {
    fn read_batch(&self, batch_offset: usize, batch_rows: usize) -> PolarsResult<DataFrame> {
        let mut columns = Vec::with_capacity(self.column_info.len());
        for (idx, info) in self.column_info.iter().enumerate() {
            columns.push(series_from_stata_column(
                idx + 1,
                info,
                self.start_row + batch_offset,
                batch_rows,
            )?);
        }
        Ok(DataFrame::from_iter(columns))
    }
}

pub fn write_from_stata(
    path: &str,
    varlist: &str,
    n_rows: usize,
    offset: usize,
    sql_if: Option<&str>,
    mapping: &str,
    _parallel_strategy: Option<crate::utilities::ParallelizationStrategy>,
    partition_by: &str,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    _compress: bool,
    _compress_string: bool,
    batch_size: usize,
) -> Result<i32, Box<dyn Error>> {
    let selected_vars_owned;
    let selected_vars = if varlist.is_empty() || varlist == "from_macro" {
        selected_vars_owned = get_macro("varlist", false, Some(10 * 1024 * 1024));
        selected_vars_owned.as_str()
    } else {
        varlist
    };

    let all_columns = if mapping.is_empty() || mapping == "from_macros" {
        let var_count = get_macro("var_count", false, None).parse::<usize>()?;
        column_info_from_macros(var_count)
    } else {
        return Err("JSON mapping is not implemented for save path".into());
    };

    let info_by_name: HashMap<&str, &StataColumnInfo> = all_columns
        .iter()
        .map(|info| (info.name.as_str(), info))
        .collect();

    let selected_names: Vec<&str> = selected_vars.split_whitespace().collect();
    let selected_infos: Vec<StataColumnInfo> = if selected_names.is_empty() {
        all_columns.clone()
    } else {
        selected_names
            .iter()
            .map(|name| {
                *info_by_name
                    .get(*name)
                    .unwrap_or_else(|| panic!("Missing macro metadata for variable {}", name))
            })
            .cloned()
            .collect()
    };

    let total_rows = n_obs() as usize;
    let start_row = offset.min(total_rows);
    let rows_available = total_rows - start_row;
    let rows_to_read = if n_rows == 0 {
        rows_available
    } else {
        n_rows.min(rows_available)
    };

    let final_batch_size = if batch_size == 0 {
        determine_optimal_batch_size(&selected_infos)
    } else {
        batch_size
    };

    let scan = StataDataScan::new(selected_infos, start_row, rows_to_read, final_batch_size);
    let mut lf = LazyFrame::anonymous_scan(Arc::new(scan), ScanArgsAnonymous::default())?;

    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", lf);
        let translated = stata_to_sql(sql);
        lf = ctx.execute(&format!("select * from df where {}", translated))?;
    }

    let partition_cols: Vec<PlSmallStr> = partition_by
        .split_whitespace()
        .map(PlSmallStr::from)
        .collect();

    let dtmeta_json = extract_dtmeta();

    if partition_cols.is_empty() {
        write_single_dataframe(
            path,
            lf,
            compression,
            compression_level,
            overwrite_partition,
            &dtmeta_json,
        )?;
    } else {
        let mut df = lf.collect()?;
        write_partitioned_dataframe(
            path,
            &mut df,
            compression,
            compression_level,
            &partition_cols,
            overwrite_partition,
            &dtmeta_json,
        )?;
    }

    Ok(0)
}

fn write_single_dataframe(
    path: &str,
    lf: LazyFrame,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
    dtmeta_json: &str,
) -> Result<(), Box<dyn Error>> {
    let out_path = Path::new(path);

    if out_path.exists() && !overwrite_partition {
        return Err(format!("Output path exists and overwrite is disabled: {}", path).into());
    }

    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent)?;
        }
    }

    if out_path.exists() && overwrite_partition {
        match std::fs::remove_file(out_path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(Box::new(e)),
        }
    }

    let tmp_path = format!("{}.tmp", path);
    let key_value_metadata =
        KeyValueMetadata::from_static(vec![(DTMETA_KEY.to_string(), dtmeta_json.to_string())]);

    let mut write_options = ParquetWriteOptions::default();
    write_options.compression = parquet_compression(compression, compression_level)?;
    write_options.key_value_metadata = Some(key_value_metadata);

    let sink_target = SinkTarget::Path(PlPath::new(&tmp_path));
    lf.sink_parquet(sink_target, write_options, None, SinkOptions::default())?
        .collect()?;

    match std::fs::rename(&tmp_path, path) {
        Ok(()) => {}
        Err(_) => {
            if out_path.exists() {
                let _ = std::fs::remove_file(out_path);
            }
            std::fs::copy(&tmp_path, path)?;
            std::fs::remove_file(&tmp_path)?;
        }
    }
    Ok(())
}

fn write_partitioned_dataframe(
    path: &str,
    df: &mut DataFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &[PlSmallStr],
    overwrite_partition: bool,
    dtmeta_json: &str,
) -> Result<(), Box<dyn Error>> {
    let out_path = Path::new(path);

    if out_path.exists() {
        if !overwrite_partition {
            return Err(format!("Output path exists and overwrite is disabled: {}", path).into());
        }

        if out_path.is_file() {
            std::fs::remove_file(out_path)?;
        } else {
            std::fs::remove_dir_all(out_path)?;
        }
    }

    create_dir_all(out_path)?;

    let mut write_options = ParquetWriteOptions::default();
    write_options.compression = parquet_compression(compression, compression_level)?;
    let key_value_metadata =
        KeyValueMetadata::from_static(vec![(DTMETA_KEY.to_string(), dtmeta_json.to_string())]);
    write_options.key_value_metadata = Some(key_value_metadata);

    write_partitioned_dataset(
        df,
        PlPathRef::Local(out_path),
        partition_by.to_vec(),
        &write_options,
        None,
        100_000,
    )?;

    Ok(())
}

fn determine_optimal_batch_size(infos: &[StataColumnInfo]) -> usize {
    // Target ~128 MB per batch for a balance between speed and memory overhead
    const TARGET_BATCH_BYTES: usize = 128 * 1024 * 1024;
    const MIN_BATCH_SIZE: usize = 5_000;
    const MAX_BATCH_SIZE: usize = 500_000;

    let row_width: usize = infos
        .iter()
        .map(|info| match info.dtype.as_str() {
            "byte" => 1,
            "int" => 2,
            "long" | "float" => 4,
            "double" => 8,
            "strl" => 128, // Conservatively estimate average strL size
            _ if info.dtype.starts_with("str") => info.str_length + 1,
            _ => 8,
        })
        .sum();

    if row_width == 0 {
        return 100_000;
    }

    let batch_size = TARGET_BATCH_BYTES / row_width;
    let clamped = batch_size.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);

    // Round to nearest 5,000 for cleaner boundaries
    (clamped / 5000) * 5000
}

fn parquet_compression(
    compression: &str,
    compression_level: Option<usize>,
) -> Result<ParquetCompression, Box<dyn Error>> {
    if compression_level.is_some() {
        return Err("compression levels are not supported; pass -1".into());
    }

    match compression {
        "lz4" => Ok(ParquetCompression::Lz4Raw),
        "uncompressed" => Ok(ParquetCompression::Uncompressed),
        "snappy" => Ok(ParquetCompression::Snappy),
        "gzip" => Ok(ParquetCompression::Gzip(None)),
        "lzo" => Ok(ParquetCompression::Lzo),
        "brotli" => Ok(ParquetCompression::Brotli(None)),
        _ => Ok(ParquetCompression::Zstd(None)),
    }
}

fn column_info_from_macros(n_vars: usize) -> Vec<StataColumnInfo> {
    (1..=n_vars)
        .map(|i| StataColumnInfo {
            name: get_macro(&format!("name_{}", i), false, None),
            dtype: get_macro(&format!("dtype_{}", i), false, None).to_lowercase(),
            format: get_macro(&format!("format_{}", i), false, None).to_lowercase(),
            str_length: get_macro(&format!("str_length_{}", i), false, None)
                .parse::<usize>()
                .unwrap_or(0),
        })
        .collect()
}

fn series_from_stata_column(
    stata_col_index: usize,
    info: &StataColumnInfo,
    offset: usize,
    n_rows: usize,
) -> Result<Series, PolarsError> {
    if info.dtype == "strl" {
        let values: Vec<Option<String>> = (0..n_rows)
            .map(|row_idx| read_string_strl(stata_col_index, offset + row_idx + 1).ok())
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if info.dtype.starts_with("str") {
        let width = info.str_length.max(1);
        let values: Vec<String> = (0..n_rows)
            .map(|row_idx| read_string(stata_col_index, offset + row_idx + 1, width))
            .collect();
        return Ok(Series::new((&info.name).into(), values));
    }

    if info.format.starts_with("%td") {
        let values: Vec<Option<i32>> = (0..n_rows)
            .map(|row_idx| {
                read_numeric(stata_col_index, offset + row_idx + 1)
                    .map(|v| v as i32 - DAY_SHIFT_SAS_STATA)
            })
            .collect();
        return Series::new((&info.name).into(), values).cast(&DataType::Date);
    }

    if info.format.starts_with("%tc") {
        let values: Vec<Option<i64>> = (0..n_rows)
            .map(|row_idx| {
                read_numeric(stata_col_index, offset + row_idx + 1).map(|v| {
                    v as i64 - ((SEC_SHIFT_SAS_STATA as f64) * (SEC_MILLISECOND as f64)) as i64
                })
            })
            .collect();
        return Series::new((&info.name).into(), values)
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None));
    }

    match info.dtype.as_str() {
        "byte" => {
            let values: Vec<Option<i8>> = (0..n_rows)
                .map(|row_idx| read_numeric(stata_col_index, offset + row_idx + 1).map(|v| v as i8))
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "int" => {
            let values: Vec<Option<i16>> = (0..n_rows)
                .map(|row_idx| {
                    read_numeric(stata_col_index, offset + row_idx + 1).map(|v| v as i16)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "long" => {
            let values: Vec<Option<i32>> = (0..n_rows)
                .map(|row_idx| {
                    read_numeric(stata_col_index, offset + row_idx + 1).map(|v| v as i32)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        "float" => {
            let values: Vec<Option<f32>> = (0..n_rows)
                .map(|row_idx| {
                    read_numeric(stata_col_index, offset + row_idx + 1).map(|v| v as f32)
                })
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
        _ => {
            let values: Vec<Option<f64>> = (0..n_rows)
                .map(|row_idx| read_numeric(stata_col_index, offset + row_idx + 1))
                .collect();
            Ok(Series::new((&info.name).into(), values))
        }
    }
}
