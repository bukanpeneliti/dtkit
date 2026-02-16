use glob::glob;
use polars::prelude::*;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use walkdir::WalkDir;

use crate::config::SCHEMA_VALIDATION_SAMPLE_ROWS;
use crate::stata_interface::{display, set_macro, ST_retcode};

#[derive(Debug, Serialize)]
pub struct DescribeFieldPayload {
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "s")]
    pub stata_type: String,
    #[serde(rename = "p")]
    pub polars_type: String,
    #[serde(rename = "l")]
    pub string_length: usize,
    #[serde(rename = "r")]
    pub rename: String,
}

#[derive(Debug, Serialize)]
pub struct DescribeSchemaPayload {
    #[serde(rename = "v")]
    pub protocol_version: u32,
    #[serde(rename = "f")]
    pub fields: Vec<DescribeFieldPayload>,
}

pub fn validate_parquet_schema(path: &str, expected_columns: &[&str]) -> Result<(), String> {
    let file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let mut reader = ParquetReader::new(file);
    let schema = reader
        .schema()
        .map_err(|e| format!("Failed to read schema: {:?}", e))?;

    let parquet_columns: HashSet<&str> = schema.iter_names().map(|s| s.as_str()).collect();

    let missing: Vec<&str> = expected_columns
        .iter()
        .filter(|col| !parquet_columns.contains(*col))
        .copied()
        .collect();

    if !missing.is_empty() {
        return Err(format!("Missing columns in parquet file: {:?}", missing));
    }

    Ok(())
}

pub fn sample_parquet_schema(path: &str) -> Result<Schema, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = ParquetReader::new(file);

    let sample_df = reader
        .with_slice(Some((0, SCHEMA_VALIDATION_SAMPLE_ROWS)))
        .finish()
        .map_err(|e| format!("Failed to read sample: {:?}", e))?;

    Ok(sample_df.schema().as_ref().clone())
}

pub fn set_schema_macros(
    schema: &Schema,
    string_lengths: &std::collections::HashMap<String, usize>,
    detailed: bool,
    quietly: bool,
) -> PolarsResult<usize> {
    if !quietly {
        display("Variable Name                    | Polars Type                      | Stata Type");
        display("-------------------------------- | -------------------------------- | ----------");
    }

    let mut payload_fields = Vec::with_capacity(schema.len());
    for (i, (name, dtype)) in schema.iter().enumerate() {
        let polars_type = match dtype {
            DataType::Boolean => "int8",
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "uint16",
            DataType::UInt32 => "uint32",
            DataType::UInt64 => "uint64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Date => "int32",
            DataType::Time => "int64",
            DataType::Datetime(_, _) => "int64",
            DataType::String => "string",
            DataType::Categorical(_, _) => "categorical",
            DataType::Enum(_, _) => "enum",
            DataType::Binary => "binary",
            _ => "string",
        };

        let is_string_like = matches!(
            dtype,
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _)
        );
        let string_length = if detailed {
            *string_lengths.get(name.as_str()).unwrap_or(&0)
        } else if is_string_like {
            2045
        } else {
            0
        };
        let stata_type = match dtype {
            DataType::Boolean => "byte",
            DataType::Int8 => "byte",
            DataType::Int16 => "int",
            DataType::Int32 => "long",
            DataType::Int64 => "double",
            DataType::UInt8 => "int",
            DataType::UInt16 => "long",
            DataType::UInt32 => "double",
            DataType::UInt64 => "double",
            DataType::Float32 => "float",
            DataType::Float64 => "double",
            DataType::Date => "date",
            DataType::Time => "time",
            DataType::Datetime(_, _) => "datetime",
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                if detailed && string_length > 2045 {
                    "strl"
                } else {
                    "string"
                }
            }
            DataType::Binary => "binary",
            _ => "strl",
        };

        if !quietly {
            display(&format!(
                "{:<32} | {:<32} | {}",
                name,
                format!("{:?}", dtype),
                stata_type
            ));
        }

        set_macro(&format!("name_{}", i + 1), name, false);
        set_macro(&format!("type_{}", i + 1), stata_type, false);
        set_macro(&format!("polars_type_{}", i + 1), polars_type, false);
        set_macro(
            &format!("string_length_{}", i + 1),
            &string_length.to_string(),
            false,
        );
        set_macro(&format!("rename_{}", i + 1), "", false);

        payload_fields.push(DescribeFieldPayload {
            name: name.to_string(),
            stata_type: stata_type.to_string(),
            polars_type: polars_type.to_string(),
            string_length,
            rename: String::new(),
        });
    }

    let payload = DescribeSchemaPayload {
        protocol_version: crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
        fields: payload_fields,
    };
    let payload_json = serde_json::to_string(&payload).map_err(|e| {
        PolarsError::ComputeError(format!("failed to encode schema payload: {e}").into())
    })?;
    set_macro(
        "schema_protocol_version",
        &crate::SCHEMA_HANDOFF_PROTOCOL_VERSION.to_string(),
        false,
    );
    set_macro("schema_payload", &payload_json, false);

    Ok(schema.len())
}

pub fn file_summary(
    path: &str,
    quietly: bool,
    detailed: bool,
    sql_if: Option<&str>,
    safe_relaxed: bool,
    asterisk_to_variable_name: Option<&str>,
    _compress: bool,
    _compress_string_to_numeric: bool,
) -> ST_retcode {
    set_macro("cast_json", "", false);

    let _ = sql_if;
    let _ = safe_relaxed;
    let _ = asterisk_to_variable_name;

    let file = match File::open(path) {
        Ok(v) => v,
        Err(e) => {
            display(&format!("Error opening parquet file: {}", e));
            return 198;
        }
    };
    let df = match ParquetReader::new(file).finish() {
        Ok(v) => v,
        Err(e) => {
            display(&format!("Error reading parquet file: {:?}", e));
            return 198;
        }
    };

    let schema = df.schema();
    let mut string_lengths = std::collections::HashMap::new();
    if detailed {
        for (name, dtype) in schema.iter() {
            if dtype.is_string() {
                if let Ok(col) = df.column(name) {
                    if let Ok(ca) = col.str() {
                        let max_len = ca
                            .into_iter()
                            .map(|v| v.map(|x| x.len()).unwrap_or(0))
                            .max()
                            .unwrap_or(0);
                        string_lengths.insert(name.to_string(), max_len);
                    }
                }
            }
        }
    }

    let n_rows = df.height();

    match set_schema_macros(&schema, &string_lengths, detailed, quietly) {
        Ok(count) => {
            set_macro("n_columns", &count.to_string(), false);
            set_macro("n_rows", &n_rows.to_string(), false);
            0
        }
        Err(e) => {
            display(&format!("Error setting schema macros: {:?}", e));
            198
        }
    }
}

pub fn verify_parquet_path(path: &str) -> bool {
    let path_obj = Path::new(path);
    if path_obj.exists() && path_obj.is_file() {
        return true;
    }
    if path_obj.exists() && path_obj.is_dir() {
        return has_parquet_files_in_hive_structure(path);
    }
    if path.contains('*') || path.contains('?') || path.contains('[') {
        let normalized_pattern = if cfg!(windows) {
            path.replace('\\', "/")
        } else {
            path.to_string()
        };
        return glob(&normalized_pattern)
            .map(|p| p.filter_map(Result::ok).next().is_some())
            .unwrap_or(false);
    }
    false
}

fn has_parquet_files_in_hive_structure(dir_path: &str) -> bool {
    let dir = Path::new(dir_path);
    if !dir.is_dir() {
        return false;
    }

    for entry in WalkDir::new(dir)
        .max_depth(3)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext.eq_ignore_ascii_case("parquet") {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::verify_parquet_path;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(tag: &str, ext: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("dtparquet_schema_{tag}_{stamp}.{ext}"))
    }

    #[test]
    fn verify_parquet_path_false_for_missing_path() {
        assert!(!verify_parquet_path(
            "C:/definitely/missing/path/file.parquet"
        ));
    }

    #[test]
    fn verify_parquet_path_true_for_existing_file() {
        let path = temp_path("exists", "parquet");
        fs::write(&path, b"test").unwrap();
        assert!(verify_parquet_path(&path.to_string_lossy()));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn verify_parquet_path_true_for_matching_glob() {
        let path = temp_path("glob", "parquet");
        fs::write(&path, b"test").unwrap();
        let dir = path.parent().unwrap();
        let pattern = format!("{}/*.parquet", dir.to_string_lossy());
        assert!(verify_parquet_path(&pattern));
        fs::remove_file(path).unwrap();
    }
}
