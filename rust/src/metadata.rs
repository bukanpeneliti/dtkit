use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;

use crate::stata_interface::{get_macro, set_macro};

pub const DTMETA_KEY: &str = "dtparquet.dtmeta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtMeta {
    pub schema_version: i32,
    pub min_reader_version: i32,
    pub vars: Vec<VarMeta>,
    pub value_labels: Vec<ValueLabelMeta>,
    pub dta_label: String,
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

pub fn extract_dtmeta() -> String {
    let var_count = get_macro("dtmeta_var_count", false, None)
        .parse::<usize>()
        .unwrap_or(0);
    let vars = (1..=var_count)
        .map(|i| VarMeta {
            name: get_macro(&format!("dtmeta_varname_{}", i), false, None),
            stata_type: get_macro(&format!("dtmeta_vartype_{}", i), false, None),
            format: get_macro(&format!("dtmeta_varfmt_{}", i), false, None),
            var_label: get_macro(&format!("dtmeta_varlab_{}", i), false, Some(65_536)),
            value_label: get_macro(&format!("dtmeta_vallab_{}", i), false, None),
        })
        .collect::<Vec<_>>();

    let lbl_count = get_macro("dtmeta_label_count", false, None)
        .parse::<usize>()
        .unwrap_or(0);
    let value_labels = (1..=lbl_count)
        .map(|i| ValueLabelMeta {
            name: get_macro(&format!("dtmeta_label_name_{}", i), false, None),
            value: get_macro(&format!("dtmeta_label_value_{}", i), false, None)
                .parse::<i64>()
                .unwrap_or(0),
            text: get_macro(&format!("dtmeta_label_text_{}", i), false, Some(65_536)),
        })
        .collect::<Vec<_>>();

    let meta = DtMeta {
        schema_version: 1,
        min_reader_version: 1,
        vars,
        value_labels,
        dta_label: get_macro("dtmeta_dta_label", false, Some(65_536)),
    };

    serde_json::to_string(&meta).unwrap_or_default()
}

pub fn load_dtmeta_from_parquet(parquet_path: &str) -> Option<DtMeta> {
    let file = File::open(parquet_path).ok()?;
    let mut reader = BufReader::new(file);
    let metadata = parquet2::read::read_metadata(&mut reader).ok()?;
    let kv = metadata.key_value_metadata.as_ref()?;
    let dtmeta_text = kv
        .iter()
        .find(|entry| entry.key == DTMETA_KEY)
        .and_then(|entry| entry.value.as_deref())?;
    serde_json::from_str::<DtMeta>(dtmeta_text).ok()
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
}
