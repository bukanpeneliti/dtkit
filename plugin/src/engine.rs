use glob::glob;
use polars::error::to_compute_err;
use polars::prelude::*;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use crate::error::DtparquetError;
use crate::filter::*;
use crate::logic::*;
use crate::transfer::*;

// --- Core Engine Types ---

#[derive(Copy, Clone)]
enum EngineStage {
    ScanPlan,
    Execute,
    StataSink,
}
impl EngineStage {
    fn as_str(&self) -> &'static str {
        match self {
            EngineStage::ScanPlan => "scan_plan",
            EngineStage::Execute => "execute",
            EngineStage::StataSink => "stata_sink",
        }
    }
}
fn set_engine_stage(prefix: &str, stage: EngineStage) {
    set_macro(&format!("{prefix}_engine_stage"), stage.as_str(), true);
}

#[derive(Debug)]
pub struct ReadScanPlan {
    pub selected_column_list: Vec<String>,
    pub transfer_columns: Vec<TransferColumnSpec>,
    pub can_use_eager: bool,
    pub schema_handoff_mode: &'static str,
}

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

#[derive(Debug, Clone)]
pub struct ReadArgs {
    pub file_path: String,
    pub varlist: String,
    pub start_row: usize,
    pub max_rows: usize,
    pub sql_if: Option<String>,
    pub sort_by: String,
    pub parallel_strategy: Option<BatchMode>,
    pub safe_relaxed: bool,
    pub asterisk_to_variable_name: Option<String>,
    pub order_by: String,
    pub stata_offset: usize,
    pub random_share: f64,
    pub random_seed: u64,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct SaveArgs {
    pub file_path: String,
    pub varlist: String,
    pub start_row: usize,
    pub max_rows: usize,
    pub sql_if: Option<String>,
    pub sort_by: String,
    pub partition_by: String,
    pub compression_codec: String,
    pub compression_level: Option<usize>,
    pub include_labels: bool,
    pub include_notes: bool,
    pub overwrite: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct DescribeArgs {
    pub file_path: String,
    pub detailed: bool,
    pub memory_savvy: bool,
}

#[derive(Debug, Clone)]
pub struct HasMetadataKeyArgs {
    pub file_path: String,
    pub key: String,
}
#[derive(Debug, Clone)]
pub struct LoadMetaArgs {
    pub file_path: String,
}

#[derive(Debug, Clone)]
pub enum CommandArgs {
    SetupCheck,
    Version,
    Read(ReadArgs),
    Save(SaveArgs),
    Describe(DescribeArgs),
    HasMetadataKey(HasMetadataKeyArgs),
    LoadMeta(LoadMetaArgs),
}

// --- Helpers ---

type ParseResult<T> = Result<T, DtparquetError>;
fn parse_arg<T: FromStr>(field: &'static str, value: &str) -> ParseResult<T> {
    value
        .parse::<T>()
        .map_err(|_| DtparquetError::InvalidArg(field, value.to_string()))
}

// --- Command Parsers & Handlers ---

pub fn parse_command(name: &str, args: &[&str]) -> ParseResult<CommandArgs> {
    match name {
        "setup_check" => Ok(CommandArgs::SetupCheck),
        "version" => Ok(CommandArgs::Version),
        "read" => parse_read_args(args),
        "save" => parse_save_args(args),
        "describe" => parse_describe_args(args),
        "has_metadata_key" => parse_has_metadata_key_args(args),
        "load_meta" => parse_load_meta_args(args),
        _ => Err(DtparquetError::SubcommandUnknown(name.to_string())),
    }
}

fn parse_read_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 16 {
        return Err(DtparquetError::SubcommandArgCount("read", 16));
    }
    let _ = parse_arg::<usize>("order_by_type", args[10])?;
    let _ = parse_arg::<f64>("order_descending", args[11])?;
    let file_path = verify_parquet_path(args[0])
        .then(|| args[0].to_string())
        .ok_or_else(|| DtparquetError::FileNotFound(args[0].to_string()))?;
    Ok(CommandArgs::Read(ReadArgs {
        file_path,
        varlist: args[1].to_string(),
        start_row: parse_arg("start_row", args[2])?,
        max_rows: parse_arg("max_rows", args[3])?,
        sql_if: (!args[4].is_empty()).then(|| args[4].to_string()),
        sort_by: args[5].to_string(),
        parallel_strategy: match args[6] {
            "columns" => Some(BatchMode::ByColumn),
            "rows" => Some(BatchMode::ByRow),
            _ => None,
        },
        safe_relaxed: args[7] == "1",
        asterisk_to_variable_name: (!args[8].is_empty()).then(|| args[8].to_string()),
        order_by: args[9].to_string(),
        stata_offset: parse_arg("stata_offset", args[12])?,
        random_share: parse_arg("random_share", args[13])?,
        random_seed: parse_arg("random_seed", args[14])?,
        batch_size: parse_arg("batch_size", args[15])?,
    }))
}

fn parse_save_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 12 {
        return Err(DtparquetError::SubcommandArgCount("save", 12));
    }
    let l: isize = parse_arg("compression_level", args[8])?;
    Ok(CommandArgs::Save(SaveArgs {
        file_path: args[0].to_string(),
        varlist: args[1].to_string(),
        start_row: parse_arg("start_row", args[2])?,
        max_rows: parse_arg("max_rows", args[3])?,
        sql_if: (!args[4].is_empty()).then(|| args[4].to_string()),
        sort_by: args[5].to_string(),
        partition_by: args[6].to_string(),
        compression_codec: args[7].to_string(),
        compression_level: (l >= 0).then_some(l as usize),
        include_labels: args[9] == "1",
        include_notes: args[10] == "1",
        overwrite: args[11] == "1",
        batch_size: if args.len() >= 13 {
            parse_arg("batch_size", args[12])?
        } else {
            0
        },
    }))
}

fn parse_describe_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 7 {
        return Err(DtparquetError::SubcommandArgCount("describe", 7));
    }
    let file_path = verify_parquet_path(args[0])
        .then(|| args[0].to_string())
        .ok_or_else(|| DtparquetError::FileNotFound(args[0].to_string()))?;
    Ok(CommandArgs::Describe(DescribeArgs {
        file_path,
        detailed: args[1] == "1",
        memory_savvy: args[2] == "1",
    }))
}

fn parse_has_metadata_key_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 2 {
        return Err(DtparquetError::SubcommandArgCount("has_metadata_key", 2));
    }
    let file_path = verify_parquet_path(args[0])
        .then(|| args[0].to_string())
        .ok_or_else(|| DtparquetError::FileNotFound(args[0].to_string()))?;
    Ok(CommandArgs::HasMetadataKey(HasMetadataKeyArgs {
        file_path,
        key: args[1].to_string(),
    }))
}

fn parse_load_meta_args(args: &[&str]) -> ParseResult<CommandArgs> {
    args.first()
        .map(|p| {
            CommandArgs::LoadMeta(LoadMetaArgs {
                file_path: p.to_string(),
            })
        })
        .ok_or(DtparquetError::SubcommandArgCount("load_meta", 1))
}

pub fn dispatch_command(cmd: CommandArgs) -> Result<ST_retcode, DtparquetError> {
    match cmd {
        CommandArgs::SetupCheck => {
            display("dtparquet Rust plugin loaded successfully");
            Ok(0)
        }
        CommandArgs::Version => {
            set_macro("dtparquet_plugin_version", env!("CARGO_PKG_VERSION"), false);
            Ok(0)
        }
        CommandArgs::Read(args) => import_parquet_request(&ReadRequest {
            path: &args.file_path,
            variables_as_str: &args.varlist,
            n_rows: args.start_row,
            offset: args.max_rows,
            sql_if: args.sql_if.as_deref(),
            mapping: &args.sort_by,
            parallel_strategy: args.parallel_strategy,
            safe_relaxed: args.safe_relaxed,
            asterisk_var: args.asterisk_to_variable_name.as_deref(),
            order_by: &args.order_by,
            stata_offset: args.stata_offset,
            random_share: args.random_share,
            random_seed: args.random_seed,
            batch_size: args.batch_size,
        }),
        CommandArgs::Save(args) => export_parquet_request(&WriteRequest {
            path: &args.file_path,
            varlist: &args.varlist,
            n_rows: args.start_row,
            offset: args.max_rows,
            sql_if: args.sql_if.as_deref(),
            mapping: &args.sort_by,
            partition_by: &args.partition_by,
            compression: &args.compression_codec,
            compression_level: args.compression_level,
            include_labels: args.include_labels,
            include_notes: args.include_notes,
            overwrite: args.overwrite,
            batch_size: args.batch_size,
        }),
        CommandArgs::Describe(args) => Ok(file_summary(
            &args.file_path,
            args.memory_savvy,
            args.detailed,
        )),
        CommandArgs::HasMetadataKey(args) => {
            let found = has_parquet_metadata_key(&args.file_path, &args.key)?;
            set_macro("has_metadata_key", if found { "1" } else { "0" }, false);
            Ok(0)
        }
        CommandArgs::LoadMeta(args) => {
            if let Some(meta) = load_dtmeta_from_parquet(&args.file_path) {
                expose_dtmeta_to_macros(&meta);
                set_macro("dtmeta_loaded", "1", false);
            } else {
                set_macro("dtmeta_loaded", "0", false);
                for m in [
                    "var_count",
                    "label_count",
                    "dta_obs",
                    "dta_vars",
                    "dta_note_count",
                    "var_note_count",
                ] {
                    set_macro(&format!("dtmeta_{m}"), "0", false);
                }
                for m in ["dta_label", "dta_ts"] {
                    set_macro(&format!("dtmeta_{m}"), "", false);
                }
            }
            Ok(0)
        }
    }
}

// --- Boundary Helpers ---

fn resolve_arg_or_macro_local(val: &str, sent: &str, m_name: &str, buf: Option<usize>) -> String {
    if val.is_empty() || val == sent {
        read_macro(m_name, false, buf)
    } else {
        val.to_string()
    }
}

#[derive(Debug, Deserialize)]
struct SchemaHandoff<T> {
    #[serde(alias = "v")]
    v: u32,
    #[serde(alias = "f")]
    f: Vec<T>,
}

fn resolve_schema_handoff_local<T, F>(
    mapping: &str,
    h_name: &str,
    exp_v: u32,
    loader: F,
) -> Result<(Vec<T>, &'static str), Box<dyn Error>>
where
    T: DeserializeOwned,
    F: FnOnce() -> Result<Vec<T>, Box<dyn Error>>,
{
    if mapping.is_empty() || mapping == "from_macros" {
        return Ok((loader()?, "legacy_macros"));
    }
    if let Ok(p) = serde_json::from_str::<SchemaHandoff<T>>(mapping) {
        if p.v != exp_v {
            return Err(format!("Protocol mismatch for {h_name}: exp {exp_v}, got {}", p.v).into());
        }
        return Ok((p.f, "json_v2"));
    }
    if let Ok(f) = serde_json::from_str::<Vec<T>>(mapping) {
        return Ok((f, "json_legacy_array"));
    }
    Err(format!("Invalid schema mapping for {h_name}").into())
}

// --- Planning Logic ---

pub struct ReadBoundaryInputs {
    pub variables_as_str: String,
    pub all_columns_unfiltered: Vec<FieldSpec>,
    pub schema_handoff_mode: &'static str,
    pub cast_json: String,
}

pub fn resolve_read_boundary_inputs(
    vars: &str,
    map: &str,
) -> Result<ReadBoundaryInputs, Box<dyn Error>> {
    let vars =
        resolve_arg_or_macro_local(vars, "from_macro", "matched_vars", Some(10 * 1024 * 1024));
    let (fields, mode) =
        resolve_schema_handoff_local(map, "read", SCHEMA_HANDOFF_PROTOCOL_VERSION, || {
            let n =
                parse_arg::<usize>("n_matched_vars", &read_macro("n_matched_vars", false, None))?;
            (1..=n)
                .map(|i| {
                    Ok(FieldSpec {
                        index: parse_arg::<usize>(
                            "v_index",
                            &read_macro(&format!("v_to_read_index_{i}"), false, None),
                        )? - 1,
                        name: read_macro(&format!("v_to_read_name_{i}"), false, None),
                        dtype: read_macro(&format!("v_to_read_p_type_{i}"), false, None),
                        stata_type: read_macro(&format!("v_to_read_type_{i}"), false, None)
                            .to_lowercase(),
                    })
                })
                .collect()
        })?;
    Ok(ReadBoundaryInputs {
        variables_as_str: vars,
        all_columns_unfiltered: fields,
        schema_handoff_mode: mode,
        cast_json: read_macro("cast_json", false, None),
    })
}

pub fn build_read_scan_plan(
    path: &str,
    b: &ReadBoundaryInputs,
    safe: bool,
    ast: Option<&str>,
    sql_if: Option<&str>,
    sort: &str,
    share: f64,
) -> Result<ReadScanPlan, Box<dyn Error>> {
    let sel_list: Vec<String> = b
        .variables_as_str
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let sel_names: HashSet<&str> = sel_list.iter().map(|s| s.as_str()).collect();
    let all_cols: Vec<FieldSpec> = b
        .all_columns_unfiltered
        .iter()
        .filter(|c| sel_names.contains(c.name.as_str()))
        .cloned()
        .collect();
    let eager = Path::new(path).is_file()
        && !path.contains('*')
        && !path.contains('?')
        && !safe
        && ast.is_none()
        && sql_if.map(|s| s.trim().is_empty()).unwrap_or(true)
        && sort.trim().is_empty()
        && share <= 0.0;
    Ok(ReadScanPlan {
        selected_column_list: sel_list,
        transfer_columns: build_transfer_columns(&all_cols),
        can_use_eager: eager,
        schema_handoff_mode: b.schema_handoff_mode,
    })
}

pub struct WriteBoundaryInputs {
    pub vars: String,
    pub all_cols: Vec<ExportField>,
    pub mode: &'static str,
}

pub fn resolve_write_boundary_inputs(
    vars: &str,
    map: &str,
) -> Result<WriteBoundaryInputs, Box<dyn Error>> {
    let vars = resolve_arg_or_macro_local(vars, "from_macro", "varlist", Some(10 * 1024 * 1024));
    let (fields, mode) =
        resolve_schema_handoff_local(map, "save", SCHEMA_HANDOFF_PROTOCOL_VERSION, || {
            let n = parse_arg::<usize>("var_count", &read_macro("var_count", false, None))?;
            (1..=n)
                .map(|i| {
                    Ok(ExportField {
                        name: read_macro(&format!("name_{i}"), false, None),
                        dtype: read_macro(&format!("dtype_{i}"), false, None).to_lowercase(),
                        format: read_macro(&format!("format_{i}"), false, None).to_lowercase(),
                        str_length: parse_arg::<usize>(
                            "str_length",
                            &read_macro(&format!("str_length_{i}"), false, None),
                        )?,
                    })
                })
                .collect()
        })?;
    Ok(WriteBoundaryInputs {
        vars,
        all_cols: fields,
        mode,
    })
}

pub fn build_write_scan_plan(
    b: &WriteBoundaryInputs,
    n_rows: usize,
    off: usize,
    part: &str,
    include_labels: bool,
    include_notes: bool,
) -> Result<WriteScanPlan, Box<dyn Error>> {
    validate_stata_schema(&b.all_cols)?;
    let by_name: HashMap<&str, &ExportField> =
        b.all_cols.iter().map(|i| (i.name.as_str(), i)).collect();
    let sel_names: Vec<&str> = b.vars.split_whitespace().collect();
    let sel_infos: Vec<ExportField> = if sel_names.is_empty() {
        b.all_cols.clone()
    } else {
        sel_names
            .iter()
            .map(|n| {
                by_name
                    .get(n)
                    .copied()
                    .ok_or_else(|| DtparquetError::Custom(format!("Missing metadata: {n}")))
            })
            .collect::<Result<Vec<&ExportField>, DtparquetError>>()?
            .into_iter()
            .cloned()
            .collect()
    };
    let total = unsafe { SF_nobs() as usize };
    let start = off.min(total);
    let to_read = if n_rows == 0 {
        total - start
    } else {
        n_rows.min(total - start)
    };
    Ok(WriteScanPlan {
        selected_infos: sel_infos.clone(),
        start_row: start,
        rows_to_read: to_read,
        row_width_bytes: sel_infos
            .iter()
            .map(|i| estimate_export_field_width_bytes(&i.dtype, i.str_length))
            .sum::<usize>()
            .max(1),
        partition_cols: part.split_whitespace().map(PlSmallStr::from).collect(),
        dtmeta_json: extract_dtmeta(include_labels, include_notes),
        schema_handoff_mode: b.mode,
    })
}

// --- Execution Runtime (Read Path) ---

pub struct ReadRequest<'a> {
    pub path: &'a str,
    pub variables_as_str: &'a str,
    pub n_rows: usize,
    pub offset: usize,
    pub sql_if: Option<&'a str>,
    pub mapping: &'a str,
    pub parallel_strategy: Option<BatchMode>,
    pub safe_relaxed: bool,
    pub asterisk_var: Option<&'a str>,
    pub order_by: &'a str,
    pub stata_offset: usize,
    pub random_share: f64,
    pub random_seed: u64,
    pub batch_size: usize,
}

pub fn import_parquet_request(req: &ReadRequest<'_>) -> Result<i32, DtparquetError> {
    let start = Instant::now();
    let (mut collects, mut processed) = (0usize, 0usize);
    init_runtime("read");

    let scan_plan_started = Instant::now();
    let boundary = resolve_read_boundary_inputs(req.variables_as_str, req.mapping)?;
    let plan = build_read_scan_plan(
        req.path,
        &boundary,
        req.safe_relaxed,
        req.asterisk_var,
        req.sql_if,
        req.order_by,
        req.random_share,
    )?;
    set_elapsed_ms_macro("read_scan_plan_elapsed_ms", scan_plan_started);
    emit_plan_macros("read", plan.schema_handoff_mode);

    let col_list: Vec<&str> = plan
        .selected_column_list
        .iter()
        .map(|s| s.as_str())
        .collect();
    let row_width_bytes = estimate_transfer_row_width_bytes(&plan.transfer_columns);
    let execute_started = Instant::now();
    let (loaded, batches, tuner) = if plan.can_use_eager {
        if !col_list.is_empty() {
            if let Err(e) = validate_parquet_schema(req.path, &col_list) {
                display(&format!("Schema validation warning: {e}"));
            }
        }
        let collect_started = Instant::now();
        let mut df = ParquetReader::new(File::open(req.path)?)
            .with_slice(Some((req.offset, req.n_rows)))
            .finish()?;
        df = df.select(
            col_list
                .iter()
                .map(|s| PlSmallStr::from(*s))
                .collect::<Vec<_>>(),
        )?;
        set_elapsed_ms_value_macro("read_open_scan_elapsed_ms", 0);
        set_elapsed_ms_macro("read_collect_elapsed_ms", collect_started);
        let read_cast_started = Instant::now();
        apply_df_casts(&mut df, &boundary.cast_json)?;
        set_elapsed_ms_macro("read_apply_cast_elapsed_ms", read_cast_started);
        set_macro("read_cast_mode", "eager", true);
        set_macro("read_cast_defer_reason", "eager_path", true);
        let mut t = AdaptiveBatchTuner::new(row_width_bytes, req.batch_size, 0);
        set_engine_stage("read", EngineStage::StataSink);
        let strategy = req.parallel_strategy.unwrap_or_else(|| {
            determine_parallelization_strategy(
                col_list.len(),
                df.height(),
                get_compute_thread_pool().current_num_threads().max(1),
            )
        });
        let sink_started = Instant::now();
        let (l, b) = sink_dataframe_in_batches(
            &df,
            0,
            &plan.transfer_columns,
            strategy,
            req.stata_offset,
            &mut t,
            &mut processed,
        )?;
        set_elapsed_ms_macro("read_sink_to_stata_elapsed_ms", sink_started);
        (l, b, t)
    } else {
        let open_scan_started = Instant::now();
        let mut lf = open_parquet_scan(req.path, req.asterisk_var)?;
        set_elapsed_ms_macro("read_open_scan_elapsed_ms", open_scan_started);
        let (cast_early, cast_mode, cast_reason) =
            should_apply_cast_early(&boundary.cast_json, req.sql_if, req.order_by);
        set_macro("read_cast_mode", cast_mode, true);
        set_macro("read_cast_defer_reason", cast_reason, true);
        if cast_early {
            let read_cast_started = Instant::now();
            lf = apply_cast(lf, &boundary.cast_json)?;
            set_elapsed_ms_macro("read_apply_cast_elapsed_ms", read_cast_started);
        } else {
            set_macro("read_apply_cast_elapsed_ms", "0", true);
        }
        lf = normalize_categorical(lf)?;
        let has_if = req.sql_if.map(|s| !s.trim().is_empty()).unwrap_or(false);
        let mut b_off = req.offset;
        if has_if {
            lf = lf.slice(req.offset as i64, req.n_rows as u32);
            b_off = 0;
        }
        let (lf_f, has_f) = apply_if_filter(lf, req.sql_if)?;
        if has_f {
            set_macro("if_filter_mode", "expr", true);
        }
        let lf_s = apply_random_sample(lf_f, req.random_share, req.random_seed, &mut collects)?;
        let mut lf_sorted = apply_sort_transform(lf_s, req.order_by);
        if !cast_early && !boundary.cast_json.is_empty() {
            let read_cast_started = Instant::now();
            lf_sorted = apply_cast(lf_sorted, &boundary.cast_json)?;
            set_elapsed_ms_macro("read_apply_cast_elapsed_ms", read_cast_started);
        }
        let mut t = AdaptiveBatchTuner::new(row_width_bytes, req.batch_size, 0);
        let n_t = get_compute_thread_pool().current_num_threads().max(1);
        let columns: Vec<Expr> = col_list.iter().map(|s| col(*s)).collect();
        let use_legacy_batches = lazy_execution_uses_legacy_batches();
        set_macro(
            "read_lazy_mode",
            if use_legacy_batches {
                "legacy_batches"
            } else {
                "single_pass"
            },
            true,
        );
        let strategy = req
            .parallel_strategy
            .unwrap_or_else(|| determine_parallelization_strategy(columns.len(), req.n_rows, n_t));
        set_engine_stage("read", EngineStage::StataSink);
        let (l, b) = run_lazy_pipeline(
            lf_sorted,
            &columns,
            req.n_rows,
            b_off,
            req.n_rows > 1_000_000,
            &plan.transfer_columns,
            strategy,
            req.stata_offset,
            &mut t,
            &mut processed,
            &mut collects,
            use_legacy_batches,
        )?;
        (l, b, t)
    };
    set_elapsed_ms_macro("read_execute_elapsed_ms", execute_started);
    finalize_runtime("read", batches, loaded, collects, processed, &tuner, start);
    Ok(0)
}

fn apply_df_casts(df: &mut DataFrame, cast_json: &str) -> PolarsResult<()> {
    if !cast_json.is_empty() {
        let m: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(cast_json).map_err(to_compute_err)?;
        if let Some(serde_json::Value::Array(cols)) = m.get("string") {
            for v in cols {
                if let serde_json::Value::String(n) = v {
                    if df.get_column_index(n).is_some() {
                        df.try_apply(n, |s| s.cast(&DataType::String))?;
                    }
                }
            }
        }
    }
    let cats: Vec<String> = df
        .schema()
        .iter()
        .filter(|(_, dt)| matches!(dt, DataType::Categorical(_, _) | DataType::Enum(_, _)))
        .map(|(n, _)| n.to_string())
        .collect();
    for n in cats {
        df.try_apply(&n, |s| s.cast(&DataType::String))?;
    }
    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CastPositionMode {
    Early,
    DeferSafe,
    DeferForce,
}

fn cast_position_mode() -> CastPositionMode {
    match env::var(ENV_CAST_POSITION_MODE)
        .unwrap_or_else(|_| "defer_safe".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "early" => CastPositionMode::Early,
        "defer_force" | "force" => CastPositionMode::DeferForce,
        _ => CastPositionMode::DeferSafe,
    }
}

fn should_apply_cast_early(
    cast_json: &str,
    sql_if: Option<&str>,
    order_by: &str,
) -> (bool, &'static str, &'static str) {
    if cast_json.is_empty() {
        return (false, "none", "empty_cast_json");
    }
    match cast_position_mode() {
        CastPositionMode::Early => (true, "early", "env_early"),
        CastPositionMode::DeferForce => (false, "defer_force", "env_defer_force"),
        CastPositionMode::DeferSafe => {
            if sql_if.map(|s| !s.trim().is_empty()).unwrap_or(false) {
                return (true, "early", "safe_mode_if_present");
            }
            if !order_by.trim().is_empty() {
                return (true, "early", "safe_mode_sort_present");
            }
            (false, "defer_safe", "safe_to_defer")
        }
    }
}

fn open_parquet_scan(path: &str, asterisk_var: Option<&str>) -> Result<LazyFrame, PolarsError> {
    if let Some(var) = asterisk_var {
        return scan_with_filename_extraction(path, var);
    }
    let source = if Path::new(path).is_dir() {
        format!("{}/**/*.parquet", normalize_scan_pattern(path))
    } else {
        normalize_scan_pattern(path)
    };
    LazyFrame::scan_parquet(
        PlRefPath::new(&source),
        ScanArgsParquet {
            allow_missing_columns: true,
            cache: false,
            ..Default::default()
        },
    )
}

fn normalize_categorical(lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
    let cat_exprs: Vec<Expr> = lf
        .clone()
        .collect_schema()?
        .iter()
        .filter(|(_, dt)| matches!(dt, DataType::Categorical(_, _) | DataType::Enum(_, _)))
        .map(|(n, _)| col(n.clone()).cast(DataType::String))
        .collect();
    Ok(if cat_exprs.is_empty() {
        lf
    } else {
        lf.with_columns(cat_exprs)
    })
}

fn normalize_scan_pattern(path: &str) -> String {
    let mut p = if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path.to_string()
    };
    if p.contains("**.") {
        p = p.replace("**.", "**/*.");
    }
    p
}

fn scan_with_filename_extraction(
    glob_path: &str,
    var_name: &str,
) -> Result<LazyFrame, PolarsError> {
    let pattern = normalize_scan_pattern(glob_path);
    let ast_pos = pattern
        .find('*')
        .ok_or_else(|| PolarsError::ComputeError("No asterisk in glob".into()))?;
    let re = Regex::new(&format!(
        "{}(.+?){}",
        regex::escape(&pattern[..ast_pos]),
        regex::escape(&pattern[ast_pos + 1..])
    ))
    .map_err(|e| PolarsError::ComputeError(format!("Invalid regex: {e}").into()))?;
    let file_paths: Vec<PathBuf> = glob(&pattern)
        .map_err(to_compute_err)?
        .collect::<Result<_, _>>()
        .map_err(to_compute_err)?;
    if file_paths.is_empty() {
        return Err(PolarsError::ComputeError(
            format!("No files: {pattern}").into(),
        ));
    }
    let lfs: Result<Vec<LazyFrame>, PolarsError> = file_paths
        .iter()
        .map(|p| {
            let p_str = p.to_string_lossy();
            let extracted_binding = normalize_scan_pattern(&p_str);
            let extracted = re
                .captures(&extracted_binding)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("unknown");
            LazyFrame::scan_parquet(
                PlRefPath::new(p_str.as_ref()),
                ScanArgsParquet {
                    allow_missing_columns: true,
                    cache: false,
                    ..Default::default()
                },
            )
            .map(|lf| lf.with_columns([smart_lit(extracted).alias(var_name)]))
        })
        .collect();
    concat(
        lfs?,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: true,
            diagonal: true,
            strict: false,
            from_partitioned_ds: true,
            maintain_order: true,
        },
    )
}

fn smart_lit(v: &str) -> Expr {
    let t = v.trim();
    if let Ok(i) = t.parse::<i64>() {
        lit(i)
    } else if let Ok(f) = t.parse::<f64>() {
        lit(f)
    } else {
        lit(v)
    }
}

fn apply_random_sample(
    lf: LazyFrame,
    share: f64,
    seed: u64,
    collects: &mut usize,
) -> Result<LazyFrame, DtparquetError> {
    if share <= 0.0 {
        return Ok(lf);
    }
    *collects += 1;
    Ok(lf
        .collect()?
        .sample_frac(
            &Series::new("frac".into(), vec![share]),
            false,
            false,
            (seed != 0).then_some(seed),
        )?
        .lazy())
}

fn apply_sort_transform(lf: LazyFrame, sort: &str) -> LazyFrame {
    if sort.is_empty() {
        return lf;
    }
    let mut cols = Vec::new();
    let mut desc = Vec::new();
    for t in sort.split_whitespace() {
        if t.starts_with('-') && t.len() > 1 {
            cols.push(PlSmallStr::from(&t[1..]));
            desc.push(true);
        } else {
            cols.push(PlSmallStr::from(t));
            desc.push(false);
        }
    }
    lf.sort(
        cols,
        SortMultipleOptions {
            descending: desc,
            ..Default::default()
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn run_lazy_pipeline(
    lf: LazyFrame,
    cols: &[Expr],
    n_rows: usize,
    src_off: usize,
    use_streaming: bool,
    trans_cols: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_off: usize,
    tuner: &mut AdaptiveBatchTuner,
    proc: &mut usize,
    collects: &mut usize,
    batch_mode: bool,
) -> PolarsResult<(usize, usize)> {
    let mut lf = lf.select(cols);
    if src_off > 0 {
        lf = lf.slice(src_off as i64, n_rows as u32);
    }
    if use_streaming {
        lf = lf.with_new_streaming(true);
        set_macro("read_streaming_enabled", "1", true);
    } else {
        set_macro("read_streaming_enabled", "0", true);
    }

    let mut collect_elapsed_ms = 0u128;
    let mut sink_elapsed_ms = 0u128;

    if batch_mode {
        let (mut off, mut loaded, mut batches) = (0, 0, 0);
        while off < n_rows {
            let b_len = (n_rows - off).min(tuner.selected_batch_size());
            let b_off = src_off + off;
            let b_lf = lf.clone().slice(b_off as i64, b_len as u32);
            let collect_started = Instant::now();
            *collects += 1;
            let b_df = b_lf.collect()?;
            collect_elapsed_ms += collect_started.elapsed().as_millis();
            if b_df.height() == 0 {
                break;
            }
            let sink_started = Instant::now();
            sink_dataframe_in_batches(
                &b_df,
                b_off - src_off,
                trans_cols,
                strategy,
                stata_off,
                tuner,
                proc,
            )?;
            sink_elapsed_ms += sink_started.elapsed().as_millis();
            loaded += b_df.height();
            batches += 1;
            off += b_len;
        }
        set_elapsed_ms_value_macro("read_collect_elapsed_ms", collect_elapsed_ms);
        set_elapsed_ms_value_macro("read_sink_to_stata_elapsed_ms", sink_elapsed_ms);
        Ok((loaded, batches))
    } else {
        let collect_started = Instant::now();
        *collects += 1;
        let df = lf.collect()?;
        collect_elapsed_ms = collect_started.elapsed().as_millis();
        let sink_started = Instant::now();
        let res = sink_dataframe_in_batches(&df, 0, trans_cols, strategy, stata_off, tuner, proc);
        sink_elapsed_ms = sink_started.elapsed().as_millis();
        set_elapsed_ms_value_macro("read_collect_elapsed_ms", collect_elapsed_ms);
        set_elapsed_ms_value_macro("read_sink_to_stata_elapsed_ms", sink_elapsed_ms);
        res
    }
}

// --- Execution Runtime (Write Path) ---

pub struct WriteRequest<'a> {
    pub path: &'a str,
    pub varlist: &'a str,
    pub n_rows: usize,
    pub offset: usize,
    pub sql_if: Option<&'a str>,
    pub mapping: &'a str,
    pub partition_by: &'a str,
    pub compression: &'a str,
    pub compression_level: Option<usize>,
    pub include_labels: bool,
    pub include_notes: bool,
    pub overwrite: bool,
    pub batch_size: usize,
}

pub fn export_parquet_request(req: &WriteRequest<'_>) -> Result<i32, DtparquetError> {
    let start = Instant::now();
    let mut collects = 0usize;
    init_runtime("write");
    set_macro("write_pipeline_mode", "legacy_direct", true);
    if write_pipeline_mode() == WritePipelineMode::ProducerConsumer {
        display("dtparquet: queue write mode is deprecated; using direct mode");
    }

    let boundary = resolve_write_boundary_inputs(req.varlist, req.mapping)?;
    let plan = build_write_scan_plan(
        &boundary,
        req.n_rows,
        req.offset,
        req.partition_by,
        req.include_labels,
        req.include_notes,
    )?;
    emit_plan_macros("write", plan.schema_handoff_mode);

    let has_if_filter = req.sql_if.map(|s| !s.trim().is_empty()).unwrap_or(false);

    if plan.partition_cols.is_empty() && !has_if_filter {
        let stats = write_single_dataframe_direct_batches(&DirectWriteRequest {
            path: req.path,
            column_info: &plan.selected_infos,
            start_row: plan.start_row,
            n_rows: plan.rows_to_read,
            configured_batch_size: req.batch_size,
            row_width_bytes: plan.row_width_bytes,
            comp: req.compression,
            level: req.compression_level,
            overwrite: req.overwrite,
            meta: &plan.dtmeta_json,
            all_numeric: plan
                .selected_infos
                .iter()
                .all(is_direct_numeric_export_field),
        })?;
        finalize_runtime(
            "write",
            stats.planned_batches,
            stats.loaded_rows,
            collects,
            stats.processed_batches,
            &stats.tuner,
            start,
        );
        return Ok(0);
    }

    let scan = Arc::new(StataRowSource::new(
        plan.selected_infos.clone(),
        plan.start_row,
        plan.rows_to_read,
        req.batch_size,
        plan.row_width_bytes,
    ));
    let mut lf = LazyFrame::anonymous_scan(scan.clone(), ScanArgsAnonymous::default())?;
    if let Some(e) = req
        .sql_if
        .filter(|s| !s.trim().is_empty())
        .map(compile_if_expr)
        .transpose()?
    {
        lf = lf.filter(e);
        set_macro("if_filter_mode", "expr", true);
    }

    if plan.partition_cols.is_empty() {
        write_single_dataframe(
            req.path,
            lf,
            req.compression,
            req.compression_level,
            req.overwrite,
            &plan.dtmeta_json,
            &mut collects,
        )?
    } else {
        write_partitioned_dataframe(
            req.path,
            lf,
            req.compression,
            req.compression_level,
            &plan.partition_cols,
            req.overwrite,
            &plan.dtmeta_json,
        )?
    }

    scan.join_pipeline_worker();
    finalize_runtime_write(&scan, collects, start);
    Ok(0)
}

fn build_parquet_write_opts(
    comp: &str,
    level: Option<usize>,
    meta: &str,
) -> Result<ParquetWriteOptions, DtparquetError> {
    Ok(ParquetWriteOptions {
        compression: parquet_compression(comp, level)?,
        key_value_metadata: Some(KeyValueMetadata::from_static(vec![(
            DTMETA_KEY.to_string(),
            meta.to_string(),
        )])),
        ..Default::default()
    })
}

fn write_single_dataframe(
    path: &str,
    lf: LazyFrame,
    comp: &str,
    level: Option<usize>,
    overwrite: bool,
    meta: &str,
    collects: &mut usize,
) -> Result<(), DtparquetError> {
    let (out, tmp) = prepare_single_output_path(path, overwrite)?;
    let opts = build_parquet_write_opts(comp, level, meta)?;
    let collect_started = Instant::now();
    *collects += 1;
    let mut df = lf.collect()?;
    set_elapsed_ms_macro("write_collect_elapsed_ms", collect_started);
    let parquet_started = Instant::now();
    let f = File::create(&tmp)?;
    ParquetWriter::new(f)
        .with_compression(opts.compression)
        .with_key_value_metadata(opts.key_value_metadata)
        .finish(&mut df)?;

    commit_tmp_output(&tmp, &out, path)?;
    set_elapsed_ms_macro("write_parquet_elapsed_ms", parquet_started);
    Ok(())
}

struct DirectWriteStats {
    planned_batches: usize,
    processed_batches: usize,
    loaded_rows: usize,
    tuner: AdaptiveBatchTuner,
}

struct DirectWriteRequest<'a> {
    path: &'a str,
    column_info: &'a [ExportField],
    start_row: usize,
    n_rows: usize,
    configured_batch_size: usize,
    row_width_bytes: usize,
    comp: &'a str,
    level: Option<usize>,
    overwrite: bool,
    meta: &'a str,
    all_numeric: bool,
}

fn write_single_dataframe_direct_batches(
    req: &DirectWriteRequest<'_>,
) -> Result<DirectWriteStats, DtparquetError> {
    let (out, tmp) = prepare_single_output_path(req.path, req.overwrite)?;
    let opts = build_parquet_write_opts(req.comp, req.level, req.meta)?;
    let parquet_started = Instant::now();

    let mut tuner = AdaptiveBatchTuner::new(req.row_width_bytes, req.configured_batch_size, 0);
    let write_batch_floor = 250_000usize;
    let initial_batch_size = tuner.selected_batch_size().max(1);
    let planned_batches = if req.n_rows == 0 {
        0
    } else {
        req.n_rows.div_ceil(initial_batch_size)
    };

    let mut processed_batches = 0usize;
    let mut loaded_rows = 0usize;
    let mut offset = 0usize;
    let mut writer: Option<polars::io::parquet::write::BatchedWriter<File>> = None;
    let mut batch_schema: Option<Schema> = None;

    let write_result = (|| -> Result<(), DtparquetError> {
        while offset < req.n_rows {
            let request_rows = (req.n_rows - offset)
                .min(tuner.selected_batch_size().max(write_batch_floor).max(1));
            let batch_started = Instant::now();
            let df = if req.all_numeric {
                read_batch_numeric_from_columns(
                    req.column_info,
                    req.start_row + offset,
                    request_rows,
                )?
            } else {
                read_batch_from_columns(req.column_info, req.start_row + offset, request_rows)?
            };
            let rows = df.height();
            if rows == 0 {
                offset += request_rows;
                continue;
            }

            if let Some(schema) = &batch_schema {
                validate_batch_schema(schema, df.schema())?;
            } else {
                batch_schema = Some(df.schema().as_ref().clone());
            }

            if writer.is_none() {
                let file = File::create(&tmp)?;
                writer = Some(
                    ParquetWriter::new(file)
                        .with_compression(opts.compression)
                        .with_key_value_metadata(opts.key_value_metadata.clone())
                        .with_row_group_size(Some(tuner.selected_batch_size().max(1)))
                        .batched(df.schema())?,
                );
            }

            if let Some(ref mut batched) = writer {
                batched.write_batch(&df)?;
            }

            processed_batches += 1;
            loaded_rows += rows;
            offset += rows;
            tuner.observe_batch(rows, batch_started.elapsed().as_millis());
        }

        if let Some(batched) = writer {
            batched.finish()?;
        } else {
            let mut empty_df = if req.all_numeric {
                read_batch_numeric_from_columns(req.column_info, req.start_row, 0)?
            } else {
                read_batch_from_columns(req.column_info, req.start_row, 0)?
            };
            let file = File::create(&tmp)?;
            ParquetWriter::new(file)
                .with_compression(opts.compression)
                .with_key_value_metadata(opts.key_value_metadata)
                .finish(&mut empty_df)?;
        }

        commit_tmp_output(&tmp, &out, req.path)?;
        Ok(())
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(err);
    }

    set_macro("write_collect_elapsed_ms", "0", true);
    set_elapsed_ms_macro("write_parquet_elapsed_ms", parquet_started);

    Ok(DirectWriteStats {
        planned_batches,
        processed_batches,
        loaded_rows,
        tuner,
    })
}

fn prepare_single_output_path(
    path: &str,
    overwrite: bool,
) -> Result<(PathBuf, PathBuf), DtparquetError> {
    let out = PathBuf::from(path);
    if let Some(parent) = out.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent)?;
        }
    }
    if out.exists() {
        if !overwrite {
            return Err(format!("Path exists: {path}").into());
        }
        let _ = std::fs::remove_file(&out);
    }
    Ok((out, PathBuf::from(format!("{path}.tmp"))))
}

fn commit_tmp_output(tmp: &Path, out: &Path, path: &str) -> std::io::Result<()> {
    std::fs::rename(tmp, path).or_else(|_| {
        if out.exists() {
            let _ = std::fs::remove_file(out);
        }
        std::fs::copy(tmp, path).and_then(|_| std::fs::remove_file(tmp))
    })
}

fn validate_batch_schema(expected: &Schema, actual: &Schema) -> Result<(), DtparquetError> {
    if expected == actual {
        return Ok(());
    }
    Err(DtparquetError::Custom(
        "Schema changed between direct write batches".to_string(),
    ))
}

fn is_direct_numeric_export_field(info: &ExportField) -> bool {
    matches!(
        info.dtype.as_str(),
        "byte" | "int" | "long" | "float" | "double"
    ) && !is_stata_date_format(&info.format)
        && !is_stata_datetime_format(&info.format)
}

fn write_partitioned_dataframe(
    path: &str,
    lf: LazyFrame,
    comp: &str,
    level: Option<usize>,
    part: &[PlSmallStr],
    overwrite: bool,
    meta: &str,
) -> Result<(), DtparquetError> {
    let parquet_started = Instant::now();
    let out = Path::new(path);
    if out.exists() {
        if !overwrite {
            return Err(format!("Path exists: {path}").into());
        }
        if out.is_file() {
            std::fs::remove_file(out)?;
        } else {
            std::fs::remove_dir_all(out)?;
        }
    }
    create_dir_all(out)?;
    let opts = build_parquet_write_opts(comp, level, meta)?;
    let collect_started = Instant::now();
    let mut df = lf.collect()?;
    set_elapsed_ms_macro("write_collect_elapsed_ms", collect_started);
    write_partitioned_dataset_local(&mut df, out, part, &opts)?;
    set_elapsed_ms_macro("write_parquet_elapsed_ms", parquet_started);
    Ok(())
}

fn write_partitioned_dataset_local(
    df: &mut DataFrame,
    out_dir: &Path,
    partition_by: &[PlSmallStr],
    opts: &ParquetWriteOptions,
) -> Result<(), DtparquetError> {
    let get_partition_path = |part_df: &DataFrame| -> Result<PathBuf, DtparquetError> {
        let mut dir = out_dir.to_path_buf();
        for name in partition_by {
            let casted = part_df
                .column(name.as_str())?
                .slice(0, 1)
                .cast(&DataType::String)?;
            let value = casted.str()?.get(0).unwrap_or("__HIVE_DEFAULT_PARTITION__");
            dir.push(format!("{}={}", name, encode_partition_value(value)));
        }
        Ok(dir)
    };

    let mut part_idx = 0usize;
    for mut part_df in df.partition_by_stable(partition_by.to_vec(), true)? {
        let partition_dir = get_partition_path(&part_df)?;
        create_dir_all(&partition_dir)?;
        let file_path = partition_dir.join("00000000.parquet");
        let file = File::create(&file_path)?;
        ParquetWriter::new(file)
            .with_compression(opts.compression)
            .with_key_value_metadata(opts.key_value_metadata.clone())
            .finish(&mut part_df)?;
        part_idx += 1;
    }

    if part_idx == 0 {
        return Err(DtparquetError::Custom(
            "partitioned write produced no groups".to_string(),
        ));
    }
    Ok(())
}

fn encode_partition_value(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

fn parquet_compression(c: &str, l: Option<usize>) -> Result<ParquetCompression, DtparquetError> {
    let codec = c.trim().to_ascii_lowercase();
    let with_no_level = |out: ParquetCompression| -> Result<ParquetCompression, DtparquetError> {
        if l.is_some() {
            return Err(DtparquetError::Custom(
                "compression levels are not supported in current build; use codec presets fast|balanced|archive"
                    .to_string(),
            ));
        }
        Ok(out)
    };

    match codec.as_str() {
        "fast" => with_no_level(ParquetCompression::Lz4Raw),
        "balanced" => with_no_level(ParquetCompression::Zstd(None)),
        "archive" => with_no_level(ParquetCompression::Brotli(None)),
        "lz4" => with_no_level(ParquetCompression::Lz4Raw),
        "uncompressed" => with_no_level(ParquetCompression::Uncompressed),
        "snappy" => with_no_level(ParquetCompression::Snappy),
        "gzip" => with_no_level(ParquetCompression::Gzip(None)),
        "brotli" => with_no_level(ParquetCompression::Brotli(None)),
        _ => with_no_level(ParquetCompression::Zstd(None)),
    }
}

// --- Internal Helpers ---

fn emit_init_macros(prefix: &str) {
    for m in [
        "selected_batch_size",
        "batch_row_width_bytes",
        "batch_memory_cap_rows",
        "batch_adjustments",
    ] {
        set_macro(&format!("{prefix}_{m}"), "0", true);
    }
    set_macro(&format!("{prefix}_batch_tuner_mode"), "fixed", true);
    set_macro(&format!("{prefix}_schema_handoff"), "legacy_macros", true);
    set_macro("if_filter_mode", "none", true);
    if prefix == "read" {
        set_macro("read_lazy_mode", "none", true);
        set_macro("read_streaming_enabled", "0", true);
        set_macro("read_cast_mode", "none", true);
        set_macro("read_cast_defer_reason", "none", true);
        set_macro("read_scan_plan_elapsed_ms", "0", true);
        set_macro("read_open_scan_elapsed_ms", "0", true);
        set_macro("read_collect_elapsed_ms", "0", true);
        set_macro("read_apply_cast_elapsed_ms", "0", true);
        set_macro("read_sink_to_stata_elapsed_ms", "0", true);
        set_macro("read_execute_elapsed_ms", "0", true);
    } else if prefix == "write" {
        set_macro("write_collect_elapsed_ms", "0", true);
        set_macro("write_parquet_elapsed_ms", "0", true);
    }
    set_engine_stage(prefix, EngineStage::ScanPlan);
}
fn emit_plan_macros(prefix: &str, mode: &str) {
    set_macro(&format!("{prefix}_schema_handoff"), mode, true);
    set_engine_stage(prefix, EngineStage::Execute);
}
fn init_runtime(prefix: &str) {
    warm_thread_pools();
    reset_transfer_metrics();
    CommonRuntimeMetrics::zero().emit_to_macros(prefix);
    emit_init_macros(prefix);
}

fn set_elapsed_ms_macro(name: &str, started: Instant) {
    set_macro(name, &started.elapsed().as_millis().to_string(), true);
}

fn set_elapsed_ms_value_macro(name: &str, elapsed_ms: u128) {
    set_macro(name, &elapsed_ms.to_string(), true);
}

fn emit_runtime_common(
    prefix: &str,
    collects: usize,
    planned: usize,
    processed: usize,
    start: Instant,
) {
    let mut m = CommonRuntimeMetrics::zero();
    m.collect_calls = collects;
    m.planned_batches = planned;
    m.processed_batches = processed;
    m.collect(start);
    m.emit_to_macros(prefix);
}

fn finalize_runtime(
    prefix: &str,
    batches: usize,
    loaded: usize,
    collects: usize,
    proc: usize,
    tuner: &AdaptiveBatchTuner,
    start: Instant,
) {
    set_engine_stage(prefix, EngineStage::StataSink);
    CommonBatchTunerMetrics::from_tuner(tuner).emit_to_macros(prefix);
    emit_runtime_common(prefix, collects, batches, proc, start);
    for (m, v) in [
        ("n_batches", batches.to_string()),
        ("loaded_rows", loaded.to_string()),
        ("n_loaded_rows", loaded.to_string()),
    ] {
        set_macro(m, &v, false);
    }
}
fn finalize_runtime_write(scan: &StataRowSource, collects: usize, start: Instant) {
    set_engine_stage("write", EngineStage::StataSink);
    CommonBatchTunerMetrics::from_tuner(&scan.batch_tuner_snapshot()).emit_to_macros("write");
    emit_runtime_common(
        "write",
        collects,
        scan.planned_batches(),
        scan.processed_batches(),
        start,
    );
}
fn lazy_execution_uses_legacy_batches() -> bool {
    env::var(ENV_LAZY_EXECUTION_MODE)
        .map(|m| {
            let l = m.trim().to_lowercase();
            l == "legacy_batches" || l == "legacy" || l == "clone_slice_collect"
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn temp_parquet_file(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dtparquet_{tag}_{}.parquet",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, b"test").unwrap();
        path
    }
    #[test]
    fn parse_read_args_ok() {
        let p = temp_parquet_file("read_ok");
        let p_s = p.to_string_lossy().to_string();
        let args = vec![
            p_s.as_str(),
            "id v",
            "1",
            "500",
            "id > 10",
            "",
            "rows",
            "1",
            "",
            "id",
            "0",
            "0",
            "2",
            "0.25",
            "42",
            "2500",
        ];
        if let CommandArgs::Read(r) = parse_read_args(&args).unwrap() {
            assert_eq!(r.file_path, p_s);
            assert_eq!(r.varlist, "id v");
            assert_eq!(r.start_row, 1);
            assert_eq!(r.max_rows, 500);
            assert_eq!(r.sql_if.as_deref(), Some("id > 10"));
            assert!(matches!(r.parallel_strategy, Some(BatchMode::ByRow)));
            assert_eq!(r.order_by, "id");
            assert_eq!(r.random_seed, 42);
            assert_eq!(r.batch_size, 2500);
        } else {
            panic!("Expected read args");
        }
        fs::remove_file(p).unwrap();
    }
    #[test]
    fn parse_save_args_ok() {
        let args = vec![
            "out.parquet",
            "id v",
            "10",
            "20",
            "id > 0",
            "from_macros",
            "region",
            "gzip",
            "-1",
            "1",
            "0",
            "1",
            "4096",
        ];
        if let CommandArgs::Save(s) = parse_save_args(&args).unwrap() {
            assert_eq!(s.file_path, "out.parquet");
            assert_eq!(s.varlist, "id v");
            assert_eq!(s.compression_codec, "gzip");
            assert_eq!(s.compression_level, None);
            assert_eq!(s.batch_size, 4096);
        } else {
            panic!("Expected save args");
        }
    }
    #[test]
    fn dispatch_metadata_failure() {
        let cmd = CommandArgs::HasMetadataKey(HasMetadataKeyArgs {
            file_path: "C:/missing.parquet".to_string(),
            key: "key".to_string(),
        });
        assert_eq!(dispatch_command(cmd).unwrap_err().to_retcode(), 198);
    }
}
