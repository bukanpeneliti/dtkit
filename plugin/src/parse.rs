use crate::commands::{
    CommandArgs, DescribeArgs, HasMetadataKeyArgs, LoadMetaArgs, ReadArgs, SaveArgs,
};
use crate::error::DtparquetError;
use crate::read;
use crate::utilities::BatchMode;

pub type ParseResult<T> = Result<T, DtparquetError>;

fn parse_parallel_strategy(s: &str) -> Option<BatchMode> {
    match s {
        "columns" => Some(BatchMode::ByColumn),
        "rows" => Some(BatchMode::ByRow),
        _ => None,
    }
}

fn parse_usize_arg(field: &'static str, value: &str) -> ParseResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| DtparquetError::InvalidArg(field, value.to_string()))
}

fn parse_u64_arg(field: &'static str, value: &str) -> ParseResult<u64> {
    value
        .parse::<u64>()
        .map_err(|_| DtparquetError::InvalidArg(field, value.to_string()))
}

fn parse_f64_arg(field: &'static str, value: &str) -> ParseResult<f64> {
    value
        .parse::<f64>()
        .map_err(|_| DtparquetError::InvalidArg(field, value.to_string()))
}

pub fn parse_read_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 13 {
        return Err(DtparquetError::SubcommandArgCount("read", 13));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(DtparquetError::FileNotFound(file_path));
    }

    let parallel_strategy = parse_parallel_strategy(args[6]);
    let safe_relaxed = args[7] == "1";
    let asterisk_to_variable_name = if args[8].is_empty() {
        None
    } else {
        Some(args[8].to_string())
    };

    let batch_size = if args.len() >= 14 {
        parse_usize_arg("batch_size", args[13])?
    } else {
        50_000
    };

    Ok(CommandArgs::Read(ReadArgs {
        file_path,
        varlist: args[1].to_string(),
        start_row: parse_usize_arg("start_row", args[2])?,
        max_rows: parse_usize_arg("max_rows", args[3])?,
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
        order_by_type: parse_usize_arg("order_by_type", args[10])?,
        order_descending: parse_f64_arg("order_descending", args[11])?,
        random_seed: parse_u64_arg("random_seed", args[12])?,
        batch_size,
    }))
}

pub fn parse_save_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 12 {
        return Err(DtparquetError::SubcommandArgCount("save", 12));
    }

    let compression_level_raw: isize = args[8]
        .parse()
        .map_err(|_| DtparquetError::InvalidArg("compression_level", args[8].to_string()))?;

    let compression_level = if compression_level_raw < 0 {
        None
    } else {
        Some(compression_level_raw as usize)
    };

    let batch_size = if args.len() >= 13 {
        parse_usize_arg("batch_size", args[12])?
    } else {
        0
    };

    Ok(CommandArgs::Save(SaveArgs {
        file_path: args[0].to_string(),
        varlist: args[1].to_string(),
        start_row: parse_usize_arg("start_row", args[2])?,
        max_rows: parse_usize_arg("max_rows", args[3])?,
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

pub fn parse_describe_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 7 {
        return Err(DtparquetError::SubcommandArgCount("describe", 7));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(DtparquetError::FileNotFound(file_path));
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

pub fn parse_has_metadata_key_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.len() < 2 {
        return Err(DtparquetError::SubcommandArgCount("has_metadata_key", 2));
    }

    let file_path = args[0].to_string();
    if !read::verify_parquet_path(&file_path) {
        return Err(DtparquetError::FileNotFound(file_path));
    }

    Ok(CommandArgs::HasMetadataKey(HasMetadataKeyArgs {
        file_path,
        key: args[1].to_string(),
    }))
}

pub fn parse_load_meta_args(args: &[&str]) -> ParseResult<CommandArgs> {
    if args.is_empty() {
        return Err(DtparquetError::SubcommandArgCount("load_meta", 1));
    }

    Ok(CommandArgs::LoadMeta(LoadMetaArgs {
        file_path: args[0].to_string(),
    }))
}

pub fn parse_command(
    subfunction_name: &str,
    subfunction_args: &[&str],
) -> ParseResult<CommandArgs> {
    match subfunction_name {
        "setup_check" => Ok(CommandArgs::SetupCheck),
        "read" => parse_read_args(subfunction_args),
        "save" => parse_save_args(subfunction_args),
        "describe" => parse_describe_args(subfunction_args),
        "has_metadata_key" => parse_has_metadata_key_args(subfunction_args),
        "load_meta" => parse_load_meta_args(subfunction_args),
        _ => Err(DtparquetError::SubcommandUnknown(
            subfunction_name.to_string(),
        )),
    }
}
