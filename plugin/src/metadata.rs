use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;

use crate::stata_interface::{read_macro, set_macro};

pub const DTMETA_KEY: &str = "dtparquet.dtmeta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtMeta {
    pub schema_version: i32,
    pub min_reader_version: i32,
    pub vars: Vec<VarMeta>,
    pub value_labels: Vec<ValueLabelMeta>,
    pub dta_label: String,
    #[serde(default)]
    pub dta_obs: i64,
    #[serde(default)]
    pub dta_vars: i64,
    #[serde(default)]
    pub dta_ts: String,
    #[serde(default)]
    pub dta_notes: Vec<String>,
    #[serde(default)]
    pub var_notes: Vec<VarNoteMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarMeta {
    pub name: String,
    pub stata_type: String,
    pub format: String,
    pub var_label: String,
    pub value_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueLabelMeta {
    pub name: String,
    pub value: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarNoteMeta {
    pub varname: String,
    pub text: String,
}

fn parse_macro_usize_or_zero(name: &str) -> usize {
    let raw = read_macro(name, false, None);
    if raw.trim().is_empty() {
        return 0;
    }
    raw.parse::<usize>()
        .unwrap_or_else(|_| panic!("Invalid macro {}='{}': expected usize", name, raw))
}

fn parse_macro_i64(name: &str) -> i64 {
    let raw = read_macro(name, false, None);
    raw.parse::<i64>()
        .unwrap_or_else(|_| panic!("Invalid macro {}='{}': expected i64", name, raw))
}

fn parse_macro_i64_or_zero(name: &str) -> i64 {
    let raw = read_macro(name, false, None);
    if raw.trim().is_empty() {
        return 0;
    }
    raw.parse::<i64>()
        .unwrap_or_else(|_| panic!("Invalid macro {}='{}': expected i64", name, raw))
}

pub fn extract_dtmeta() -> String {
    let var_count = parse_macro_usize_or_zero("dtmeta_var_count");
    let vars = (1..=var_count)
        .map(|i| VarMeta {
            name: read_macro(&format!("dtmeta_varname_{}", i), false, None),
            stata_type: read_macro(&format!("dtmeta_vartype_{}", i), false, None),
            format: read_macro(&format!("dtmeta_varfmt_{}", i), false, None),
            var_label: read_macro(&format!("dtmeta_varlab_{}", i), false, Some(65_536)),
            value_label: read_macro(&format!("dtmeta_vallab_{}", i), false, None),
        })
        .collect::<Vec<_>>();

    let lbl_count = parse_macro_usize_or_zero("dtmeta_label_count");
    let value_labels = (1..=lbl_count)
        .map(|i| ValueLabelMeta {
            name: read_macro(&format!("dtmeta_label_name_{}", i), false, None),
            value: parse_macro_i64(&format!("dtmeta_label_value_{}", i)),
            text: read_macro(&format!("dtmeta_label_text_{}", i), false, Some(65_536)),
        })
        .collect::<Vec<_>>();

    let meta = DtMeta {
        schema_version: 1,
        min_reader_version: 1,
        vars,
        value_labels,
        dta_label: read_macro("dtmeta_dta_label", false, Some(65_536)),
        dta_obs: parse_macro_i64_or_zero("dtmeta_dta_obs"),
        dta_vars: parse_macro_i64_or_zero("dtmeta_dta_vars"),
        dta_ts: read_macro("dtmeta_dta_ts", false, Some(65_536)),
        dta_notes: {
            let count = parse_macro_usize_or_zero("dtmeta_dta_note_count");
            (1..=count)
                .map(|i| read_macro(&format!("dtmeta_dta_note_{}", i), false, Some(65_536)))
                .collect::<Vec<_>>()
        },
        var_notes: {
            let count = parse_macro_usize_or_zero("dtmeta_var_note_count");
            (1..=count)
                .map(|i| VarNoteMeta {
                    varname: read_macro(&format!("dtmeta_var_note_var_{}", i), false, None),
                    text: read_macro(&format!("dtmeta_var_note_text_{}", i), false, Some(65_536)),
                })
                .collect::<Vec<_>>()
        },
    };

    serde_json::to_string(&meta).unwrap_or_default()
}

pub fn load_dtmeta_from_parquet(parquet_path: &str) -> Option<DtMeta> {
    let file = File::open(parquet_path).ok()?;
    let mut reader = ParquetReader::new(file);
    let metadata = reader.get_metadata().ok()?.clone();
    let kv = metadata.key_value_metadata().as_ref()?;
    let dtmeta_text = kv
        .iter()
        .find(|entry| entry.key == DTMETA_KEY)
        .and_then(|entry| entry.value.as_deref())?;
    serde_json::from_str::<DtMeta>(dtmeta_text).ok()
}

pub fn has_parquet_metadata_key(parquet_path: &str, key: &str) -> Result<bool, Box<dyn Error>> {
    let file = File::open(parquet_path)?;
    let mut reader = ParquetReader::new(file);
    let metadata = reader.get_metadata().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to read parquet metadata: {:?}", e),
        )
    })?;

    Ok(metadata
        .key_value_metadata()
        .as_ref()
        .map(|kv| kv.iter().any(|entry| entry.key == key))
        .unwrap_or(false))
}

pub fn expose_dtmeta_to_macros(meta: &DtMeta) {
    set_macro("dtmeta_var_count", &meta.vars.len().to_string(), false);
    for (i, var) in meta.vars.iter().enumerate() {
        let idx = i + 1;
        set_macro(&format!("dtmeta_varname_{}", idx), &var.name, false);
        set_macro(&format!("dtmeta_vartype_{}", idx), &var.stata_type, false);
        set_macro(&format!("dtmeta_varfmt_{}", idx), &var.format, false);
        set_macro(&format!("dtmeta_varlab_{}", idx), &var.var_label, false);
        set_macro(&format!("dtmeta_vallab_{}", idx), &var.value_label, false);
    }

    set_macro(
        "dtmeta_label_count",
        &meta.value_labels.len().to_string(),
        false,
    );
    for (i, lbl) in meta.value_labels.iter().enumerate() {
        let idx = i + 1;
        set_macro(&format!("dtmeta_label_name_{}", idx), &lbl.name, false);
        set_macro(
            &format!("dtmeta_label_value_{}", idx),
            &lbl.value.to_string(),
            false,
        );
        set_macro(&format!("dtmeta_label_text_{}", idx), &lbl.text, false);
    }

    set_macro("dtmeta_dta_label", &meta.dta_label, false);
    set_macro("dtmeta_dta_obs", &meta.dta_obs.to_string(), false);
    set_macro("dtmeta_dta_vars", &meta.dta_vars.to_string(), false);
    set_macro("dtmeta_dta_ts", &meta.dta_ts, false);
    set_macro(
        "dtmeta_dta_note_count",
        &meta.dta_notes.len().to_string(),
        false,
    );
    for (i, note) in meta.dta_notes.iter().enumerate() {
        let idx = i + 1;
        set_macro(&format!("dtmeta_dta_note_{}", idx), note, false);
    }

    set_macro(
        "dtmeta_var_note_count",
        &meta.var_notes.len().to_string(),
        false,
    );
    for (i, note) in meta.var_notes.iter().enumerate() {
        let idx = i + 1;
        set_macro(
            &format!("dtmeta_var_note_var_{}", idx),
            &note.varname,
            false,
        );
        set_macro(&format!("dtmeta_var_note_text_{}", idx), &note.text, false);
    }
}
