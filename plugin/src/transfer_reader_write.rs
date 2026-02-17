use super::*;

pub fn write_numeric_column_range(
    col: &Column,
    transfer_column: &TransferColumnSpec,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    stata_offset: usize,
) -> PolarsResult<()> {
    match col.dtype() {
        DataType::Boolean => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_boolean_to_f64,
        ),
        DataType::Int8 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i8_to_f64,
        ),
        DataType::Int16 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i16_to_f64,
        ),
        DataType::Int32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i32_to_f64,
        ),
        DataType::Int64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_i64_to_f64,
        ),
        DataType::UInt8 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u8_to_f64,
        ),
        DataType::UInt16 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u16_to_f64,
        ),
        DataType::UInt32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u32_to_f64,
        ),
        DataType::UInt64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_u64_to_f64,
        ),
        DataType::Float32 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_f32_to_f64,
        ),
        DataType::Float64 => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_f64_to_f64,
        ),
        DataType::Date => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_date_to_stata_days,
        ),
        DataType::Time => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_time_to_stata_millis,
        ),
        DataType::Datetime(_, _) => write_numeric_with_converter(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
            convert_datetime_to_stata_clock,
        ),
        DataType::Null => {
            write_all_missing_numeric_range(
                transfer_column,
                start_index,
                start_row,
                end_row,
                stata_offset,
            );
            Ok(())
        }
        _ => {
            record_transfer_conversion_failure();
            Err(dtype_mismatch_error(
                col,
                transfer_column,
                "numeric/date/time/datetime",
            ))
        }
    }
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

fn is_date_dtype(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Date)
}

fn is_time_dtype(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Time)
}

fn is_datetime_dtype(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Datetime(_, _))
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
            is_date_dtype,
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
            is_time_dtype,
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
            is_datetime_dtype,
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
