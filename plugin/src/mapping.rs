use polars::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    #[serde(alias = "i")]
    pub index: usize,
    #[serde(alias = "n")]
    pub name: String,
    #[serde(alias = "d")]
    pub dtype: String,
    #[serde(alias = "s")]
    pub stata_type: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransferWriterKind {
    Numeric,
    Date,
    Time,
    Datetime,
    String,
    Strl,
}

pub fn transfer_writer_kind_from_stata_type(stata_type: &str) -> TransferWriterKind {
    match stata_type {
        "string" => TransferWriterKind::String,
        "strl" => TransferWriterKind::Strl,
        "date" => TransferWriterKind::Date,
        "time" => TransferWriterKind::Time,
        "datetime" => TransferWriterKind::Datetime,
        _ => TransferWriterKind::Numeric,
    }
}

pub fn is_stata_string_dtype(dtype: &str) -> bool {
    dtype == "strl" || dtype.starts_with("str")
}

pub fn is_stata_date_format(format: &str) -> bool {
    format.starts_with("%td")
}

pub fn is_stata_datetime_format(format: &str) -> bool {
    format.starts_with("%tc")
}

pub fn export_field_polars_dtype(dtype: &str, format: &str) -> DataType {
    let base = match dtype {
        "byte" => DataType::Int8,
        "int" => DataType::Int16,
        "long" => DataType::Int32,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        _ if is_stata_string_dtype(dtype) => DataType::String,
        _ => DataType::Float64,
    };

    if is_stata_date_format(format) {
        DataType::Date
    } else if is_stata_datetime_format(format) {
        DataType::Datetime(TimeUnit::Milliseconds, None)
    } else {
        base
    }
}

pub fn estimate_export_field_width_bytes(dtype: &str, str_length: usize) -> usize {
    match dtype {
        "byte" => 1,
        "int" => 2,
        "long" | "float" => 4,
        "double" => 8,
        "strl" => 128,
        _ if is_stata_string_dtype(dtype) => str_length.max(1) + 1,
        _ => 8,
    }
}

pub fn polars_to_stata_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Int8 | DataType::Int16 => "int".to_string(),
        DataType::Int32 | DataType::Int64 => "long".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::String => "strL".to_string(),
        DataType::Date => "long".to_string(),
        DataType::Datetime(_, _) => "double".to_string(),
        _ => "strL".to_string(),
    }
}

pub fn stata_to_polars_type(stata_type: &str) -> DataType {
    match stata_type {
        "byte" => DataType::Int8,
        "int" => DataType::Int16,
        "long" => DataType::Int32,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "strL" => DataType::String,
        _ if stata_type.starts_with("str") => DataType::String,
        _ => DataType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_writer_kind_maps_expected_stata_types() {
        assert_eq!(
            transfer_writer_kind_from_stata_type("string"),
            TransferWriterKind::String
        );
        assert_eq!(
            transfer_writer_kind_from_stata_type("strl"),
            TransferWriterKind::Strl
        );
        assert_eq!(
            transfer_writer_kind_from_stata_type("date"),
            TransferWriterKind::Date
        );
        assert_eq!(
            transfer_writer_kind_from_stata_type("datetime"),
            TransferWriterKind::Datetime
        );
        assert_eq!(
            transfer_writer_kind_from_stata_type("double"),
            TransferWriterKind::Numeric
        );
    }

    #[test]
    fn export_field_polars_dtype_prefers_format_over_numeric_base() {
        assert!(matches!(
            export_field_polars_dtype("double", "%td"),
            DataType::Date
        ));
        assert!(matches!(
            export_field_polars_dtype("long", "%tc"),
            DataType::Datetime(_, _)
        ));
    }

    #[test]
    fn estimate_export_field_width_bytes_handles_strings_and_numeric() {
        assert_eq!(estimate_export_field_width_bytes("byte", 0), 1);
        assert_eq!(estimate_export_field_width_bytes("double", 0), 8);
        assert_eq!(estimate_export_field_width_bytes("str10", 10), 11);
        assert_eq!(estimate_export_field_width_bytes("strl", 0), 128);
    }
}
