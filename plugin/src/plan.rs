use glob::glob;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;
use std::path::PathBuf;

use crate::boundary::{resolve_arg_or_macro, resolve_schema_handoff};
use crate::mapping::FieldSpec;
use crate::stata_interface::read_macro;
use crate::transfer::{ExportField, TransferColumnSpec};

pub mod read {

    use super::*;

    #[derive(Debug)]
    pub struct ReadScanPlan {
        pub selected_column_list: Vec<String>,
        pub transfer_columns: Vec<TransferColumnSpec>,
        pub can_use_eager: bool,
        pub schema_handoff_mode: &'static str,
    }

    pub struct ReadBoundaryInputs {
        pub variables_as_str: String,
        pub all_columns_unfiltered: Vec<FieldSpec>,
        pub schema_handoff_mode: &'static str,
        pub cast_json: String,
    }

    pub fn resolve_read_boundary_inputs(
        variables_as_str: &str,
        mapping: &str,
    ) -> Result<ReadBoundaryInputs, Box<dyn Error>> {
        let variables_as_str = resolve_arg_or_macro(
            variables_as_str,
            "from_macro",
            "matched_vars",
            Some(10 * 1024 * 1024),
        );

        let (all_columns_unfiltered, schema_handoff_mode): (Vec<FieldSpec>, &'static str) =
            resolve_schema_handoff(
                mapping,
                "read",
                crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
                || {
                    let n_vars = read_macro("n_matched_vars", false, None)
                        .parse::<usize>()
                        .map_err(|_| "Invalid macro n_matched_vars: expected usize")?;
                    column_info_from_macros(n_vars)
                },
            )?;

        Ok(ReadBoundaryInputs {
            variables_as_str,
            all_columns_unfiltered,
            schema_handoff_mode,
            cast_json: read_macro("cast_json", false, None),
        })
    }

    pub fn build_read_scan_plan(
        path: &str,
        boundary_inputs: &ReadBoundaryInputs,
        safe_relaxed: bool,
        asterisk_var: Option<&str>,
        sql_if: Option<&str>,
        sort: &str,
        random_share: f64,
    ) -> Result<ReadScanPlan, Box<dyn Error>> {
        let variables_as_str = boundary_inputs.variables_as_str.as_str();

        let selected_column_list: Vec<String> = variables_as_str
            .split_whitespace()
            .map(str::to_string)
            .collect();
        let selected_column_names: HashSet<&str> =
            selected_column_list.iter().map(|s| s.as_str()).collect();
        let all_columns: Vec<FieldSpec> = boundary_inputs
            .all_columns_unfiltered
            .iter()
            .filter(|col_info| selected_column_names.contains(col_info.name.as_str()))
            .cloned()
            .collect();

        let transfer_columns = crate::transfer::build_transfer_columns(&all_columns);
        let can_use_eager = Path::new(path).is_file()
            && !path.contains('*')
            && !path.contains('?')
            && !safe_relaxed
            && asterisk_var.is_none()
            && sql_if.map(|s| s.trim().is_empty()).unwrap_or(true)
            && sort.trim().is_empty()
            && random_share <= 0.0;

        Ok(ReadScanPlan {
            selected_column_list,
            transfer_columns,
            can_use_eager,
            schema_handoff_mode: boundary_inputs.schema_handoff_mode,
        })
    }

    pub fn open_parquet_scan(
        path: &str,
        _safe_relaxed: bool,
        asterisk_to_variable_name: Option<&str>,
    ) -> Result<LazyFrame, PolarsError> {
        if let Some(var_name) = asterisk_to_variable_name {
            return scan_with_filename_extraction(path, var_name);
        }

        let path_obj = Path::new(path);
        let source = if path_obj.is_dir() {
            format!("{}/**/*.parquet", normalize_scan_pattern(path))
        } else {
            normalize_scan_pattern(path)
        };
        scan_parquet_native(&source)
    }

    pub fn normalize_categorical(lf: &LazyFrame) -> Result<LazyFrame, PolarsError> {
        let schema = lf.clone().collect_schema()?;
        let cat_expressions: Vec<Expr> = schema
            .iter()
            .filter_map(|(name, dtype)| {
                if matches!(dtype, DataType::Categorical(_, _) | DataType::Enum(_, _)) {
                    Some(col(name.clone()).cast(DataType::String))
                } else {
                    None
                }
            })
            .collect();
        if cat_expressions.is_empty() {
            Ok(lf.clone())
        } else {
            Ok(lf.clone().with_columns(cat_expressions))
        }
    }

    fn normalize_scan_pattern(path: &str) -> String {
        let mut normalized_pattern = if cfg!(windows) {
            path.replace('\\', "/")
        } else {
            path.to_string()
        };
        if normalized_pattern.contains("**.") {
            normalized_pattern = normalized_pattern.replace("**.", "**/*.");
        }
        normalized_pattern
    }

    fn scan_parquet_native(normalized_pattern: &str) -> Result<LazyFrame, PolarsError> {
        let scan_args = ScanArgsParquet {
            allow_missing_columns: true,
            cache: false,
            ..Default::default()
        };
        LazyFrame::scan_parquet(PlPath::new(normalized_pattern), scan_args)
    }

    fn scan_with_filename_extraction(
        glob_path: &str,
        variable_name: &str,
    ) -> Result<LazyFrame, PolarsError> {
        let mut normalized_pattern = if cfg!(windows) {
            glob_path.replace('\\', "/")
        } else {
            glob_path.to_string()
        };
        if normalized_pattern.contains("**.") {
            normalized_pattern = normalized_pattern.replace("**.", "**/*.");
        }
        let asterisk_pos = normalized_pattern
            .find('*')
            .ok_or_else(|| PolarsError::ComputeError("No asterisk found in glob pattern".into()))?;
        let before_asterisk = &normalized_pattern[..asterisk_pos];
        let after_asterisk = &normalized_pattern[asterisk_pos + 1..];
        let regex_pattern = format!(
            "{}(.+?){}",
            regex::escape(before_asterisk),
            regex::escape(after_asterisk)
        );
        let re = Regex::new(&regex_pattern).map_err(|e| {
            PolarsError::ComputeError(format!("Invalid regex pattern: {}", e).into())
        })?;

        let paths = glob(&normalized_pattern).map_err(|e| {
            PolarsError::ComputeError(format!("Invalid glob pattern: {}", e).into())
        })?;
        let file_paths: Result<Vec<PathBuf>, _> = paths.collect();
        let file_paths = file_paths.map_err(|e| {
            PolarsError::ComputeError(format!("Failed to read glob results: {}", e).into())
        })?;
        if file_paths.is_empty() {
            return Err(PolarsError::ComputeError(
                format!("No files found matching pattern: {}", normalized_pattern).into(),
            ));
        }

        let scan_args = ScanArgsParquet {
            allow_missing_columns: true,
            cache: false,
            ..Default::default()
        };
        let lazy_frames: Result<Vec<LazyFrame>, PolarsError> = file_paths
            .iter()
            .map(|path| {
                let path_str = path.to_string_lossy();
                let normalized_path = if cfg!(windows) {
                    path_str.replace('\\', "/")
                } else {
                    path_str.to_string()
                };
                let extracted_value = re
                    .captures(&normalized_path)
                    .and_then(|caps| caps.get(1))
                    .map(|m| m.as_str())
                    .unwrap_or("unknown");
                LazyFrame::scan_parquet(PlPath::new(path_str.as_ref()), scan_args.clone())
                    .map(|lf| lf.with_columns([smart_lit(extracted_value).alias(variable_name)]))
            })
            .collect();

        concat(
            lazy_frames?,
            UnionArgs {
                parallel: true,
                rechunk: false,
                to_supertypes: true,
                diagonal: true,
                from_partitioned_ds: true,
                maintain_order: true,
            },
        )
    }

    fn smart_lit(value: &str) -> Expr {
        let trimmed = value.trim();
        if let Ok(int_val) = trimmed.parse::<i64>() {
            return lit(int_val);
        }
        if let Ok(float_val) = trimmed.parse::<f64>() {
            return lit(float_val);
        }
        lit(value)
    }

    fn column_info_from_macros(n_vars: usize) -> Result<Vec<FieldSpec>, Box<dyn Error>> {
        let mut column_infos = Vec::with_capacity(n_vars);
        for i in 0..n_vars {
            let index_raw = read_macro(&format!("v_to_read_index_{}", i + 1), false, None);
            let index = index_raw.parse::<usize>().map_err(|_| {
                format!(
                    "Invalid macro v_to_read_index_{}='{}': expected usize",
                    i + 1,
                    index_raw
                )
            })? - 1;
            let name = read_macro(&format!("v_to_read_name_{}", i + 1), false, None);
            let dtype = read_macro(&format!("v_to_read_p_type_{}", i + 1), false, None);
            let stata_type =
                read_macro(&format!("v_to_read_type_{}", i + 1), false, None).to_lowercase();
            column_infos.push(FieldSpec {
                index,
                name,
                dtype,
                stata_type,
            });
        }
        Ok(column_infos)
    }
}

pub mod write {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct WriteScanPlan {
        pub selected_infos: Vec<ExportField>,
        pub start_row: usize,
        pub rows_to_read: usize,
        pub row_width_bytes: usize,
        pub partition_cols: Vec<PlSmallStr>,
        pub dtmeta_json: String,
        pub schema_handoff_mode: &'static str,
    }

    pub struct WriteBoundaryInputs {
        pub selected_vars: String,
        pub all_columns: Vec<ExportField>,
        pub schema_handoff_mode: &'static str,
    }

    pub fn resolve_write_boundary_inputs(
        varlist: &str,
        mapping: &str,
    ) -> Result<WriteBoundaryInputs, Box<dyn Error>> {
        let selected_vars =
            resolve_arg_or_macro(varlist, "from_macro", "varlist", Some(10 * 1024 * 1024));

        let (all_columns, schema_handoff_mode): (Vec<ExportField>, &'static str) =
            resolve_schema_handoff(
                mapping,
                "save",
                crate::SCHEMA_HANDOFF_PROTOCOL_VERSION,
                || {
                    let var_count = read_macro("var_count", false, None).parse::<usize>()?;
                    column_info_from_macros(var_count)
                },
            )?;

        Ok(WriteBoundaryInputs {
            selected_vars,
            all_columns,
            schema_handoff_mode,
        })
    }

    pub fn build_write_scan_plan(
        boundary_inputs: &WriteBoundaryInputs,
        n_rows: usize,
        offset: usize,
        partition_by: &str,
    ) -> Result<WriteScanPlan, Box<dyn Error>> {
        let selected_vars = boundary_inputs.selected_vars.as_str();
        let all_columns = boundary_inputs.all_columns.clone();

        crate::transfer::validate_stata_schema(&all_columns)?;

        let info_by_name: HashMap<&str, &ExportField> = all_columns
            .iter()
            .map(|info| (info.name.as_str(), info))
            .collect();
        let selected_names: Vec<&str> = selected_vars.split_whitespace().collect();
        let selected_infos: Vec<ExportField> = if selected_names.is_empty() {
            all_columns.clone()
        } else {
            selected_names
                .iter()
                .map(|name| {
                    *info_by_name
                        .get(*name)
                        .unwrap_or_else(|| panic!("Missing macro metadata for variable {}", name))
                })
                .cloned()
                .collect()
        };

        use crate::stata_interface::count_rows;
        let total_rows = count_rows() as usize;
        let start_row = offset.min(total_rows);
        let rows_available = total_rows - start_row;
        let rows_to_read = if n_rows == 0 {
            rows_available
        } else {
            n_rows.min(rows_available)
        };

        let row_width_bytes = estimate_export_row_width_bytes(&selected_infos);
        let partition_cols: Vec<PlSmallStr> = partition_by
            .split_whitespace()
            .map(PlSmallStr::from)
            .collect();

        use crate::metadata::extract_dtmeta;
        let dtmeta_json = extract_dtmeta();

        Ok(WriteScanPlan {
            selected_infos,
            start_row,
            rows_to_read,
            row_width_bytes,
            partition_cols,
            dtmeta_json,
            schema_handoff_mode: boundary_inputs.schema_handoff_mode,
        })
    }

    fn column_info_from_macros(n_vars: usize) -> Result<Vec<ExportField>, Box<dyn Error>> {
        (1..=n_vars)
            .map(|i| {
                let str_length_raw = read_macro(&format!("str_length_{}", i), false, None);
                let str_length = str_length_raw.parse::<usize>().map_err(|_| {
                    format!(
                        "Invalid macro str_length_{}='{}': expected usize",
                        i, str_length_raw
                    )
                })?;
                Ok(ExportField {
                    name: read_macro(&format!("name_{}", i), false, None),
                    dtype: read_macro(&format!("dtype_{}", i), false, None).to_lowercase(),
                    format: read_macro(&format!("format_{}", i), false, None).to_lowercase(),
                    str_length,
                })
            })
            .collect()
    }

    fn estimate_export_row_width_bytes(infos: &[ExportField]) -> usize {
        infos
            .iter()
            .map(|info| {
                crate::mapping::estimate_export_field_width_bytes(&info.dtype, info.str_length)
            })
            .sum::<usize>()
            .max(1)
    }
}

pub use read::{ReadBoundaryInputs, ReadScanPlan};
pub use write::{WriteBoundaryInputs, WriteScanPlan};
