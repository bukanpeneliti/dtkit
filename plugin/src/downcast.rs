use polars::prelude::*;
use serde_json::{Map, Value};

pub fn apply_cast(df: LazyFrame, type_mapping_json: &str) -> PolarsResult<LazyFrame> {
    let type_mapping: Map<String, Value> = serde_json::from_str(type_mapping_json)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid JSON: {}", e).into()))?;
    let schema = df.clone().collect_schema()?;
    let mut cast_pairs: Vec<(String, DataType)> = Vec::new();

    for (type_str, columns_value) in type_mapping {
        if let Value::Array(columns_array) = columns_value {
            let target_type = parse_data_type(&type_str)?;
            for column_value in columns_array {
                if let Value::String(col_name) = column_value {
                    if schema.get(&col_name).is_some() {
                        cast_pairs.push((col_name, target_type.clone()));
                    }
                }
            }
        }
    }

    if cast_pairs.is_empty() {
        Ok(df)
    } else {
        let cast_map: PlHashMap<&str, DataType> = cast_pairs
            .iter()
            .map(|(name, dtype)| (name.as_str(), dtype.clone()))
            .collect();
        Ok(df.cast(cast_map, true))
    }
}

fn parse_data_type(type_str: &str) -> PolarsResult<DataType> {
    match type_str {
        "boolean" => Ok(DataType::Boolean),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" => Ok(DataType::String),
        _ => Err(PolarsError::ComputeError(
            format!("Unknown data type: {}", type_str).into(),
        )),
    }
}
