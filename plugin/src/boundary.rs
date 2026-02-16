use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::error::Error;

use crate::stata_interface::read_macro;

#[derive(Debug, Deserialize)]
struct SchemaHandoff<T> {
    #[serde(alias = "v")]
    protocol_version: u32,
    #[serde(alias = "f")]
    fields: Vec<T>,
}

pub fn resolve_arg_or_macro(
    value: &str,
    sentinel: &str,
    macro_name: &str,
    buffer_size: Option<usize>,
) -> String {
    if value.is_empty() || value == sentinel {
        read_macro(macro_name, false, buffer_size)
    } else {
        value.to_string()
    }
}

pub fn resolve_schema_handoff<T, F>(
    mapping: &str,
    handoff_name: &str,
    expected_protocol_version: u32,
    macro_loader: F,
) -> Result<(Vec<T>, &'static str), Box<dyn Error>>
where
    T: DeserializeOwned,
    F: FnOnce() -> Result<Vec<T>, Box<dyn Error>>,
{
    if mapping.is_empty() || mapping == "from_macros" {
        return Ok((macro_loader()?, "legacy_macros"));
    }

    parse_schema_handoff_fields(mapping, handoff_name, expected_protocol_version)
}

fn parse_schema_handoff_fields<T: DeserializeOwned>(
    mapping: &str,
    handoff_name: &str,
    expected_protocol_version: u32,
) -> Result<(Vec<T>, &'static str), Box<dyn Error>> {
    if let Ok(payload) = serde_json::from_str::<SchemaHandoff<T>>(mapping) {
        if payload.protocol_version != expected_protocol_version {
            return Err(format!(
                "Schema protocol mismatch for {}: expected version {}, got {}. Update ado and plugin to matching versions or retry with legacy macro handoff.",
                handoff_name,
                expected_protocol_version,
                payload.protocol_version
            )
            .into());
        }
        return Ok((payload.fields, "json_v2"));
    }

    if let Ok(fields) = serde_json::from_str::<Vec<T>>(mapping) {
        return Ok((fields, "json_legacy_array"));
    }

    Err(format!(
        "Invalid schema mapping payload for {}. Expected JSON object {{\"protocol_version\":...,\"fields\":[...]}}.",
        handoff_name
    )
    .into())
}
