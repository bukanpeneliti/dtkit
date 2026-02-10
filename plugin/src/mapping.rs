use polars::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub index: usize,
    pub name: String,
    pub dtype: String,
    pub stata_type: String,
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
        "strL" | _ if stata_type.starts_with("str") => DataType::String,
        _ => DataType::String,
    }
}
