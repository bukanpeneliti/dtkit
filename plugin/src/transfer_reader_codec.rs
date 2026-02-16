use super::*;

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

pub fn convert_boolean_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Boolean(v) => CellConversion::Value(Some(if v { 1.0 } else { 0.0 })),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_i8_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int8(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_i16_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int16(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_i32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int32(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_i64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Int64(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_u8_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt8(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_u16_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt16(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_u32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt32(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_u64_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::UInt64(v) => CellConversion::Value(Some(v as f64)),
        AnyValue::Null => CellConversion::Value(None),
        _ => CellConversion::Mismatch,
    }
}

pub fn convert_f32_to_f64(value: AnyValue<'_>) -> CellConversion<f64> {
    match value {
        AnyValue::Float32(v) => CellConversion::Value(Some(v as f64)),
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
