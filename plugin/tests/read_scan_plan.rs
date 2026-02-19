use dtparquet::engine::{build_read_scan_plan, ReadBoundaryInputs};
use dtparquet::logic::FieldSpec;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_parquet_file(tag: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("dtparquet_plan_{tag}_{stamp}.parquet"));
    fs::write(&path, b"test").unwrap();
    path
}

fn sample_boundary_inputs(vars: &str) -> ReadBoundaryInputs {
    ReadBoundaryInputs {
        variables_as_str: vars.to_string(),
        all_columns_unfiltered: vec![
            FieldSpec {
                index: 0,
                name: "id".to_string(),
                dtype: "int64".to_string(),
                stata_type: "long".to_string(),
            },
            FieldSpec {
                index: 1,
                name: "value".to_string(),
                dtype: "float64".to_string(),
                stata_type: "double".to_string(),
            },
            FieldSpec {
                index: 2,
                name: "note".to_string(),
                dtype: "string".to_string(),
                stata_type: "string".to_string(),
            },
        ],
        schema_handoff_mode: "legacy_macros",
        cast_json: String::new(),
    }
}

#[test]
fn build_read_scan_plan_selects_requested_columns_and_allows_eager() {
    let path = temp_parquet_file("eager");
    let boundary = sample_boundary_inputs("id value");

    let plan = build_read_scan_plan(
        &path.to_string_lossy(),
        &boundary,
        false,
        None,
        None,
        "",
        0.0,
    )
    .unwrap();

    assert_eq!(plan.selected_column_list, vec!["id", "value"]);
    assert_eq!(plan.transfer_columns.len(), 2);
    assert!(plan.can_use_eager);

    fs::remove_file(path).unwrap();
}

#[test]
fn build_read_scan_plan_disables_eager_when_if_filter_present() {
    let path = temp_parquet_file("lazy");
    let boundary = sample_boundary_inputs("id value");

    let plan = build_read_scan_plan(
        &path.to_string_lossy(),
        &boundary,
        false,
        None,
        Some("id > 10"),
        "",
        0.0,
    )
    .unwrap();

    assert!(!plan.can_use_eager);

    fs::remove_file(path).unwrap();
}
