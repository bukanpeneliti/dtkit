use crate::commands::{
    CommandArgs, DescribeArgs, HasMetadataKeyArgs, LoadMetaArgs, ReadArgs, SaveArgs,
};
use crate::metadata;
use crate::read;
use crate::schema;
use crate::stata_interface::{display, set_macro, ST_retcode};
use crate::write;

fn handle_setup_check() -> ST_retcode {
    display("dtparquet Rust plugin loaded successfully");
    0
}

fn build_read_request(args: &ReadArgs) -> read::ReadRequest<'_> {
    read::ReadRequest {
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
        order_by_type: args.order_by_type,
        order_descending: args.order_descending,
        stata_offset: args.stata_offset,
        random_share: args.random_share,
        random_seed: args.random_seed,
        batch_size: args.batch_size,
    }
}

fn handle_read(args: &ReadArgs) -> ST_retcode {
    let request = build_read_request(args);
    let read_result = read::import_parquet_request(&request);

    match read_result {
        Ok(code) => code,
        Err(e) => {
            display(&format!("Error reading the file = {:?}", e));
            198
        }
    }
}

fn build_write_request(args: &SaveArgs) -> write::WriteRequest<'_> {
    write::WriteRequest {
        path: &args.file_path,
        varlist: &args.varlist,
        n_rows: args.start_row,
        offset: args.max_rows,
        sql_if: args.sql_if.as_deref(),
        mapping: &args.sort_by,
        parallel_strategy: None,
        partition_by: &args.partition_by,
        compression: &args.compression_codec,
        compression_level: args.compression_level,
        overwrite_partition: args.include_labels,
        compress: args.include_notes,
        compress_string: args.overwrite,
        batch_size: args.batch_size,
    }
}

fn handle_save(args: &SaveArgs) -> ST_retcode {
    let request = build_write_request(args);
    let save_result = write::export_parquet_request(&request);

    match save_result {
        Ok(code) => code,
        Err(e) => {
            display(&format!("Error writing parquet file = {:?}", e));
            198
        }
    }
}

fn handle_describe(args: &DescribeArgs) -> ST_retcode {
    schema::file_summary(
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
    match metadata::has_parquet_metadata_key(&args.file_path, &args.key) {
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

pub fn dispatch_command(cmd: CommandArgs) -> ST_retcode {
    match cmd {
        CommandArgs::SetupCheck => handle_setup_check(),
        CommandArgs::Read(args) => handle_read(&args),
        CommandArgs::Save(args) => handle_save(&args),
        CommandArgs::Describe(args) => handle_describe(&args),
        CommandArgs::HasMetadataKey(args) => handle_has_metadata_key(&args),
        CommandArgs::LoadMeta(args) => handle_load_meta(&args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::BatchMode;

    #[test]
    fn build_read_request_preserves_legacy_mapping() {
        let args = ReadArgs {
            file_path: "in.parquet".to_string(),
            varlist: "id value".to_string(),
            start_row: 7,
            max_rows: 11,
            sql_if: Some("id > 0".to_string()),
            sort_by: "from_macros".to_string(),
            parallel_strategy: Some(BatchMode::ByColumn),
            safe_relaxed: true,
            asterisk_to_variable_name: Some("star".to_string()),
            order_by: "id".to_string(),
            order_by_type: 2,
            order_descending: 1.0,
            stata_offset: 9,
            random_share: 0.4,
            random_seed: 123,
            batch_size: 2048,
        };

        let request = build_read_request(&args);
        assert_eq!(request.path, "in.parquet");
        assert_eq!(request.variables_as_str, "id value");
        assert_eq!(request.n_rows, 7);
        assert_eq!(request.offset, 11);
        assert_eq!(request.sql_if, Some("id > 0"));
        assert_eq!(request.mapping, "from_macros");
        assert!(matches!(
            request.parallel_strategy,
            Some(BatchMode::ByColumn)
        ));
        assert!(request.safe_relaxed);
        assert_eq!(request.asterisk_var, Some("star"));
        assert_eq!(request.order_by, "id");
        assert_eq!(request.order_by_type, 2);
        assert_eq!(request.order_descending, 1.0);
        assert_eq!(request.stata_offset, 9);
        assert_eq!(request.random_share, 0.4);
        assert_eq!(request.random_seed, 123);
        assert_eq!(request.batch_size, 2048);
    }

    #[test]
    fn build_write_request_preserves_legacy_mapping() {
        let args = SaveArgs {
            file_path: "out.parquet".to_string(),
            varlist: "id value".to_string(),
            start_row: 5,
            max_rows: 13,
            sql_if: Some("id < 10".to_string()),
            sort_by: "from_macros".to_string(),
            partition_by: "region".to_string(),
            compression_codec: "zstd".to_string(),
            compression_level: Some(3),
            include_labels: true,
            include_notes: false,
            overwrite: true,
            batch_size: 4096,
        };

        let request = build_write_request(&args);
        assert_eq!(request.path, "out.parquet");
        assert_eq!(request.varlist, "id value");
        assert_eq!(request.n_rows, 5);
        assert_eq!(request.offset, 13);
        assert_eq!(request.sql_if, Some("id < 10"));
        assert_eq!(request.mapping, "from_macros");
        assert!(request.parallel_strategy.is_none());
        assert_eq!(request.partition_by, "region");
        assert_eq!(request.compression, "zstd");
        assert_eq!(request.compression_level, Some(3));
        assert!(request.overwrite_partition);
        assert!(!request.compress);
        assert!(request.compress_string);
        assert_eq!(request.batch_size, 4096);
    }
}
