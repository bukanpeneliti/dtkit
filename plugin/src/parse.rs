use crate::commands::{
    CommandArgs, DescribeArgs, HasMetadataKeyArgs, LoadMetaArgs, ReadArgs, SaveArgs,
};
use crate::config::DEFAULT_BATCH_SIZE;
use crate::error::DtparquetError;
use crate::schema;
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
    if args.len() < 16 {
        return Err(DtparquetError::SubcommandArgCount("read", 16));
    }

    let file_path = args[0].to_string();
    if !schema::verify_parquet_path(&file_path) {
        return Err(DtparquetError::FileNotFound(file_path));
    }

    let parallel_strategy = parse_parallel_strategy(args[6]);
    let safe_relaxed = args[7] == "1";
    let asterisk_to_variable_name = if args[8].is_empty() {
        None
    } else {
        Some(args[8].to_string())
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
        stata_offset: parse_usize_arg("stata_offset", args[12])?,
        random_share: parse_f64_arg("random_share", args[13])?,
        random_seed: parse_u64_arg("random_seed", args[14])?,
        batch_size: if args.len() >= 16 {
            parse_usize_arg("batch_size", args[15])?
        } else {
            DEFAULT_BATCH_SIZE
        },
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
        partition_by: args[6].to_string(),
        compression_codec: args[7].to_string(),
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
    if !schema::verify_parquet_path(&file_path) {
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
    if !schema::verify_parquet_path(&file_path) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_parquet_file(tag: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("dtparquet_{tag}_{stamp}.parquet"));
        fs::write(&path, b"test").unwrap();
        path
    }

    #[test]
    fn parse_read_args_parses_full_argument_vector() {
        let path = temp_parquet_file("parse_read_ok");
        let path_str = path.to_string_lossy().to_string();
        let args = vec![
            path_str.as_str(),
            "id value",
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

        let parsed = parse_read_args(&args).unwrap();
        match parsed {
            CommandArgs::Read(read) => {
                assert_eq!(read.file_path, path_str);
                assert_eq!(read.varlist, "id value");
                assert_eq!(read.start_row, 1);
                assert_eq!(read.max_rows, 500);
                assert_eq!(read.sql_if.as_deref(), Some("id > 10"));
                assert!(matches!(read.parallel_strategy, Some(BatchMode::ByRow)));
                assert!(read.safe_relaxed);
                assert_eq!(read.order_by, "id");
                assert_eq!(read.stata_offset, 2);
                assert_eq!(read.random_share, 0.25);
                assert_eq!(read.random_seed, 42);
                assert_eq!(read.batch_size, 2500);
            }
            _ => panic!("expected read args"),
        }

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn parse_read_args_rejects_missing_file() {
        let args = vec![
            "C:/definitely/missing/file.parquet",
            "id",
            "1",
            "10",
            "",
            "",
            "rows",
            "0",
            "",
            "",
            "0",
            "0",
            "0",
            "0",
            "1",
            "1000",
        ];

        let err = parse_read_args(&args).unwrap_err();
        assert!(matches!(err, DtparquetError::FileNotFound(_)));
    }

    #[test]
    fn parse_command_rejects_unknown_subcommand() {
        let err = parse_command("unknown_cmd", &[]).unwrap_err();
        assert!(matches!(err, DtparquetError::SubcommandUnknown(_)));
    }

    #[test]
    fn parse_save_args_parses_argument_vector_shape() {
        let args = vec![
            "out.parquet",
            "id value",
            "10",
            "20",
            "id > 0",
            "from_macros",
            "partition_col",
            "gzip",
            "-1",
            "1",
            "0",
            "1",
            "4096",
        ];

        let parsed = parse_save_args(&args).unwrap();
        match parsed {
            CommandArgs::Save(save) => {
                assert_eq!(save.file_path, "out.parquet");
                assert_eq!(save.varlist, "id value");
                assert_eq!(save.start_row, 10);
                assert_eq!(save.max_rows, 20);
                assert_eq!(save.sql_if.as_deref(), Some("id > 0"));
                assert_eq!(save.sort_by, "from_macros");
                assert_eq!(save.partition_by, "partition_col");
                assert_eq!(save.compression_codec, "gzip");
                assert_eq!(save.compression_level, None);
                assert!(save.include_labels);
                assert!(!save.include_notes);
                assert!(save.overwrite);
                assert_eq!(save.batch_size, 4096);
            }
            _ => panic!("expected save args"),
        }
    }

    #[test]
    fn parse_save_args_rejects_invalid_compression_level() {
        let args = vec![
            "out.parquet",
            "*",
            "0",
            "0",
            "",
            "from_macros",
            "",
            "zstd",
            "not_a_number",
            "1",
            "0",
            "0",
            "1024",
        ];

        let err = parse_save_args(&args).unwrap_err();
        assert!(matches!(
            err,
            DtparquetError::InvalidArg("compression_level", _)
        ));
    }
}
