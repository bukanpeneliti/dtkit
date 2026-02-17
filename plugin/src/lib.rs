use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;

pub mod engine;
pub mod error;
pub mod filter;
pub mod logic;
pub mod transfer;

pub use engine::{
    dispatch_command, parse_command, CommandArgs, DescribeArgs, HasMetadataKeyArgs, LoadMetaArgs,
    ReadArgs, SaveArgs,
};
pub use error::DtparquetError;
pub use logic::{display, ST_retcode, SCHEMA_HANDOFF_PROTOCOL_VERSION};

fn execute_subcommand(
    subfunction_name: &str,
    subfunction_args: &[&str],
) -> Result<ST_retcode, DtparquetError> {
    parse_command(subfunction_name, subfunction_args).and_then(dispatch_command)
}

#[no_mangle]
pub static mut _stata_: *mut stata_sys::ST_plugin = ptr::null_mut();

#[no_mangle]
pub extern "C" fn pginit(p: *mut stata_sys::ST_plugin) -> stata_sys::ST_retcode {
    unsafe {
        _stata_ = p;
    }
    stata_sys::SD_PLUGINVER
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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

        match execute_subcommand(subfunction_name, subfunction_args) {
            Ok(code) => code,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_subcommand_preserves_parse_errors() {
        let err = execute_subcommand("unknown_subcommand", &[]).unwrap_err();
        assert!(matches!(err, DtparquetError::SubcommandUnknown(_)));
        assert_eq!(err.to_retcode(), 198);
    }

    #[test]
    fn execute_subcommand_preserves_file_not_found_contract() {
        let err = execute_subcommand(
            "describe",
            &[
                "C:/definitely/missing/file.parquet",
                "0",
                "0",
                "",
                "",
                "0",
                "0",
            ],
        )
        .unwrap_err();
        assert!(matches!(err, DtparquetError::FileNotFound(_)));
        assert_eq!(err.to_retcode(), 601);
    }
}
