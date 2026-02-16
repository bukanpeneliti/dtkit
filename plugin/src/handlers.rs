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
        args.stata_offset,
        args.random_share,
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
