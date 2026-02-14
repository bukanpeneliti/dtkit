use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;

pub mod downcast;
pub mod mapping;
pub mod metadata;
pub mod read;
pub mod sql_from_if;
pub mod stata_interface;
pub mod utilities;
pub mod write;

use stata_interface::{display, set_macro, ST_retcode};
use utilities::BatchMode;

#[no_mangle]
pub static mut _stata_: *mut stata_sys::ST_plugin = ptr::null_mut();

#[no_mangle]
pub extern "C" fn pginit(p: *mut stata_sys::ST_plugin) -> stata_sys::ST_retcode {
    unsafe {
        _stata_ = p;
    }
    stata_sys::SD_PLUGINVER
}

#[derive(Debug, Clone)]
pub enum CommandError {
    MissingArg(&'static str),
    InvalidArg(&'static str, String),
    FileNotFound(String),
    SubcommandUnknown(String),
    SubcommandArgCount(&'static str, usize),
    IoError(String),
    Custom(&'static str),
}

impl CommandError {
    pub fn to_retcode(&self) -> ST_retcode {
        match self {
            CommandError::MissingArg(_) => 198,
            CommandError::InvalidArg(_, _) => 198,
            CommandError::SubcommandArgCount(_, _) => 198,
            CommandError::SubcommandUnknown(_) => 198,
            CommandError::FileNotFound(_) => 601,
            CommandError::IoError(_) => 198,
            CommandError::Custom(_) => 198,
        }
    }

    pub fn display_msg(&self) -> String {
        match self {
            CommandError::MissingArg(msg) => format!("Error: {}", msg),
            CommandError::InvalidArg(field, val) => {
                format!("Error: invalid {} '{}'", field, val)
            }
            CommandError::SubcommandArgCount(cmd, count) => {
                format!("Error: {} requires {} arguments", cmd, count)
            }
            CommandError::SubcommandUnknown(name) => {
                format!("Error: Unknown subfunction '{}'", name)
            }
            CommandError::FileNotFound(path) => {
                format!("File does not exist ({})", path)
            }
            CommandError::IoError(msg) => format!("Error: {}", msg),
            CommandError::Custom(msg) => format!("Error: {}", msg),
        }
    }
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
    pub order_by_type: usize,
    pub order_descending: f64,
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
    pub sorting: Option<String>,
    pub compress: bool,
    pub asterisk_to_variable_name: Option<String>,
    pub compress_string_to_numeric: bool,
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
    Read(ReadArgs),
    Save(SaveArgs),
    Describe(DescribeArgs),
    HasMetadataKey(HasMetadataKeyArgs),
    LoadMeta(LoadMetaArgs),
}

fn parse_parallel_strategy(s: &str) -> Option<BatchMode> {
    match s {
        "columns" => Some(BatchMode::ByColumn),
        "rows" => Some(BatchMode::ByRow),
        _ => None,
    }
}

fn parse_read_args(args: &[&str]) -> Result<CommandArgs, CommandError> {
    if args.len() < 13 {
        return Err(CommandError::SubcommandArgCount("read", 13));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(CommandError::FileNotFound(file_path));
    }

    let parallel_strategy = parse_parallel_strategy(args[6]);
    let safe_relaxed = args[7] == "1";
    let asterisk_to_variable_name = if args[8].is_empty() {
        None
    } else {
        Some(args[8].to_string())
    };

    let batch_size = if args.len() >= 14 {
        args[13].parse().unwrap_or(50_000)
    } else {
        50_000
    };

    Ok(CommandArgs::Read(ReadArgs {
        file_path,
        varlist: args[1].to_string(),
        start_row: args[2].parse().unwrap_or(0),
        max_rows: args[3].parse().unwrap_or(0),
        sql_if: if args[4].is_empty() {
            None
        } else {
            Some(args[4].to_string())
        },
        sort_by: args[5].to_string(),
        parallel_strategy,
        safe_relaxed,
        asterisk_to_variable_name,
        order_by: args[9].to_string(),
        order_by_type: args[10].parse().unwrap_or(0),
        order_descending: args[11].parse().unwrap_or(0.0),
        random_seed: args[12].parse().unwrap_or(0),
        batch_size,
    }))
}

fn parse_save_args(args: &[&str]) -> Result<CommandArgs, CommandError> {
    if args.len() < 12 {
        return Err(CommandError::SubcommandArgCount("save", 12));
    }

    let compression_level_raw: isize = args[8]
        .parse()
        .map_err(|_| CommandError::InvalidArg("compression_level", args[8].to_string()))?;

    let compression_level = if compression_level_raw < 0 {
        None
    } else {
        Some(compression_level_raw as usize)
    };

    let batch_size = if args.len() >= 13 {
        args[12].parse().unwrap_or(0)
    } else {
        0
    };

    Ok(CommandArgs::Save(SaveArgs {
        file_path: args[0].to_string(),
        varlist: args[1].to_string(),
        start_row: args[2].parse().unwrap_or(0),
        max_rows: args[3].parse().unwrap_or(0),
        sql_if: if args[4].is_empty() {
            None
        } else {
            Some(args[4].to_string())
        },
        sort_by: args[5].to_string(),
        compression_codec: args[6].to_string(),
        compression_level,
        include_labels: args[9] == "1",
        include_notes: args[10] == "1",
        overwrite: args[11] == "1",
        batch_size,
    }))
}

fn parse_describe_args(args: &[&str]) -> Result<CommandArgs, CommandError> {
    if args.len() < 7 {
        return Err(CommandError::SubcommandArgCount("describe", 7));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(CommandError::FileNotFound(file_path));
    }

    let asterisk_to_variable_name = if args[4].is_empty() {
        None
    } else {
        Some(args[4].to_string())
    };

    Ok(CommandArgs::Describe(DescribeArgs {
        file_path,
        detailed: args[1] == "1",
        memory_savvy: args[2] == "1",
        sorting: if args[3].is_empty() {
            None
        } else {
            Some(args[3].to_string())
        },
        compress: args[5] == "1",
        asterisk_to_variable_name,
        compress_string_to_numeric: args[6] == "1",
    }))
}

fn parse_has_metadata_key_args(args: &[&str]) -> Result<CommandArgs, CommandError> {
    if args.len() < 2 {
        return Err(CommandError::SubcommandArgCount("has_metadata_key", 2));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(CommandError::FileNotFound(file_path));
    }

    Ok(CommandArgs::HasMetadataKey(HasMetadataKeyArgs {
        file_path,
        key: args[1].to_string(),
    }))
}

fn parse_load_meta_args(args: &[&str]) -> Result<CommandArgs, CommandError> {
    if args.len() < 1 {
        return Err(CommandError::SubcommandArgCount("load_meta", 1));
    }

    Ok(CommandArgs::LoadMeta(LoadMetaArgs {
        file_path: args[0].to_string(),
    }))
}

fn parse_command(
    subfunction_name: &str,
    subfunction_args: &[&str],
) -> Result<CommandArgs, CommandError> {
    match subfunction_name {
        "setup_check" => Ok(CommandArgs::SetupCheck),
        "read" => parse_read_args(subfunction_args),
        "save" => parse_save_args(subfunction_args),
        "describe" => parse_describe_args(subfunction_args),
        "has_metadata_key" => parse_has_metadata_key_args(subfunction_args),
        "load_meta" => parse_load_meta_args(subfunction_args),
        _ => Err(CommandError::SubcommandUnknown(
            subfunction_name.to_string(),
        )),
    }
}

fn handle_setup_check() -> ST_retcode {
    display("dtparquet Rust plugin loaded successfully");
    0
}

fn handle_read(args: &ReadArgs) -> ST_retcode {
    let read_result = read::import_parquet(
        &args.file_path,
        &args.varlist,
        args.start_row,
        args.max_rows,
        args.sql_if.as_deref(),
        &args.sort_by,
        args.parallel_strategy,
        args.safe_relaxed,
        args.asterisk_to_variable_name.as_deref(),
        &args.order_by,
        args.order_by_type,
        args.order_descending,
        args.random_seed,
        args.batch_size,
    );

    match read_result {
        Ok(code) => code,
        Err(e) => {
            display(&format!("Error reading the file = {:?}", e));
            198
        }
    }
}

fn handle_save(args: &SaveArgs) -> ST_retcode {
    let save_result = write::export_parquet(
        &args.file_path,
        &args.varlist,
        args.start_row,
        args.max_rows,
        args.sql_if.as_deref(),
        &args.sort_by,
        None,
        &args.compression_codec,
        &args.compression_codec,
        args.compression_level,
        args.include_labels,
        args.include_notes,
        args.overwrite,
        args.batch_size,
    );

    match save_result {
        Ok(code) => code,
        Err(e) => {
            display(&format!("Error writing parquet file = {:?}", e));
            198
        }
    }
}

fn handle_describe(args: &DescribeArgs) -> ST_retcode {
    read::file_summary(
        &args.file_path,
        args.detailed,
        args.memory_savvy,
        args.sorting.as_deref(),
        true,
        args.asterisk_to_variable_name.as_deref(),
        args.compress,
        args.compress_string_to_numeric,
    )
}

fn handle_has_metadata_key(args: &HasMetadataKeyArgs) -> ST_retcode {
    match read::has_metadata_key(&args.file_path, &args.key) {
        Ok(found) => {
            set_macro("has_metadata_key", if found { "1" } else { "0" }, false);
            0
        }
        Err(e) => {
            display(&format!("Error checking metadata key = {:?}", e));
            198
        }
    }
}

fn handle_load_meta(args: &LoadMetaArgs) -> ST_retcode {
    if let Some(meta) = metadata::load_dtmeta_from_parquet(&args.file_path) {
        metadata::expose_dtmeta_to_macros(&meta);
        set_macro("dtmeta_loaded", "1", false);
    } else {
        set_macro("dtmeta_loaded", "0", false);
        set_macro("dtmeta_var_count", "0", false);
        set_macro("dtmeta_label_count", "0", false);
        set_macro("dtmeta_dta_label", "", false);
        set_macro("dtmeta_dta_obs", "0", false);
        set_macro("dtmeta_dta_vars", "0", false);
        set_macro("dtmeta_dta_ts", "", false);
        set_macro("dtmeta_dta_note_count", "0", false);
        set_macro("dtmeta_var_note_count", "0", false);
    }
    0
}

fn dispatch_command(cmd: CommandArgs) -> ST_retcode {
    match cmd {
        CommandArgs::SetupCheck => handle_setup_check(),
        CommandArgs::Read(args) => handle_read(&args),
        CommandArgs::Save(args) => handle_save(&args),
        CommandArgs::Describe(args) => handle_describe(&args),
        CommandArgs::HasMetadataKey(args) => handle_has_metadata_key(&args),
        CommandArgs::LoadMeta(args) => handle_load_meta(&args),
    }
}

#[no_mangle]
pub extern "C" fn stata_call(argc: c_int, argv: *const *const c_char) -> ST_retcode {
    std::panic::catch_unwind(|| {
        if argc < 1 || argv.is_null() {
            display("Error: No subfunction specified");
            return 198;
        }

        let args: Vec<&str> = unsafe {
            let arg_ptrs = slice::from_raw_parts(argv, argc as usize);
            let mut rust_args = Vec::with_capacity(argc as usize);

            for arg_ptr in arg_ptrs {
                if arg_ptr.is_null() {
                    display("Error: Null argument");
                    return 198;
                }

                match CStr::from_ptr(*arg_ptr).to_str() {
                    Ok(s) => rust_args.push(s),
                    Err(_) => {
                        display("Error: Invalid UTF-8 in argument");
                        return 198;
                    }
                }
            }

            rust_args
        };

        let subfunction_name = args[0];
        let subfunction_args = &args[1..];

        match parse_command(subfunction_name, subfunction_args) {
            Ok(cmd) => dispatch_command(cmd),
            Err(e) => {
                display(&e.display_msg());
                e.to_retcode()
            }
        }
    })
    .unwrap_or_else(|_| {
        display("Panic occurred in dtparquet plugin");
        198
    })
}
