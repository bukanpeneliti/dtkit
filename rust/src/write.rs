use polars::prelude::*;
use polars_sql::SQLContext;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::path::Path;

use crate::metadata::{extract_dtmeta, DTMETA_KEY};
use crate::stata_interface::{get_macro, n_obs, read_numeric, read_string, read_string_strl};
use crate::utilities::{DAY_SHIFT_SAS_STATA, SEC_MILLISECOND, SEC_SHIFT_SAS_STATA};

#[derive(Clone)]
struct StataColumnInfo {
    name: String,
    dtype: String,
    format: String,
    str_length: usize,
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
) -> Result<i32, Box<dyn Error>> {
    let selected_vars_owned;
    let selected_vars = if varlist.is_empty() || varlist == "from_macro" {
        selected_vars_owned = get_macro("varlist", false, Some(10 * 1024 * 1024));
        selected_vars_owned.as_str()
    } else {
        varlist
    };

    let selected_names: Vec<&str> = selected_vars.split_whitespace().collect();
    if selected_names.is_empty() {
        return Err("No variables selected for save".into());
    }

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

    let selected_infos: Vec<StataColumnInfo> = selected_names
        .iter()
        .map(|name| {
            *info_by_name
                .get(*name)
                .unwrap_or_else(|| panic!("Missing macro metadata for variable {}", name))
        })
        .cloned()
        .collect();

    let total_rows = n_obs() as usize;
    let start_row = offset.min(total_rows);
    let rows_available = total_rows - start_row;
    let rows_to_read = if n_rows == 0 {
        rows_available
    } else {
        n_rows.min(rows_available)
    };

    let mut columns = Vec::with_capacity(selected_infos.len());
    for (idx, info) in selected_infos.iter().enumerate() {
        columns.push(series_from_stata_column(
            idx + 1,
            info,
            start_row,
            rows_to_read,
        )?);
    }

    let mut df = DataFrame::from_iter(columns);

    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", df.lazy());
        df = ctx
            .execute(&format!("select * from df where {}", sql))?
            .collect()?;
    }

    let partition_cols: Vec<PlSmallStr> = partition_by
        .split_whitespace()
        .map(PlSmallStr::from)
        .collect();

    if partition_cols.is_empty() {
        write_single_dataframe(
            path,
            &mut df,
            compression,
            compression_level,
            overwrite_partition,
        )?;
    } else {
        write_partitioned_dataframe(
            path,
            &mut df,
            compression,
            compression_level,
            &partition_cols,
            overwrite_partition,
        )?;
    }

    Ok(0)
}

fn write_single_dataframe(
    path: &str,
    df: &mut DataFrame,
    compression: &str,
    compression_level: Option<usize>,
    overwrite_partition: bool,
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
        std::fs::remove_file(out_path)?;
    }

    let tmp_path = format!("{}.tmp", path);
    let mut file = File::create(&tmp_path)?;

    let key_value_metadata =
        KeyValueMetadata::from_static(vec![(DTMETA_KEY.to_string(), extract_dtmeta())]);

    let writer = ParquetWriter::new(&mut file)
        .with_compression(parquet_compression(compression, compression_level))
        .with_key_value_metadata(Some(key_value_metadata));
    writer.finish(df)?;

    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn write_partitioned_dataframe(
    path: &str,
    df: &mut DataFrame,
    compression: &str,
    compression_level: Option<usize>,
    partition_by: &[PlSmallStr],
    overwrite_partition: bool,
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
    write_options.compression = parquet_compression(compression, compression_level);

    write_partitioned_dataset(
        df,
        out_path,
        partition_by.to_vec(),
        &write_options,
        None,
        100_000,
    )?;

    Ok(())
}

fn parquet_compression(compression: &str, compression_level: Option<usize>) -> ParquetCompression {
    match compression {
        "lz4" => ParquetCompression::Lz4Raw,
        "uncompressed" => ParquetCompression::Uncompressed,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => {
            let level = compression_level.and_then(|v| GzipLevel::try_new(v as u8).ok());
            ParquetCompression::Gzip(level)
        }
        "lzo" => ParquetCompression::Lzo,
        "brotli" => {
            let level = compression_level.and_then(|v| BrotliLevel::try_new(v as u32).ok());
            ParquetCompression::Brotli(level)
        }
        _ => {
            let level = compression_level.and_then(|v| ZstdLevel::try_new(v as i32).ok());
            ParquetCompression::Zstd(level)
        }
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
