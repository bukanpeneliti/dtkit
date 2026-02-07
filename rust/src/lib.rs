use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;

pub mod downcast;
pub mod mapping;
pub mod metadata;
pub mod read;
pub mod stata_interface;
pub mod utilities;
pub mod write;

use stata_interface::{display, set_macro, ST_retcode};
use utilities::ParallelizationStrategy;

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

        match subfunction_name {
            "setup_check" => {
                display("dtparquet Rust plugin loaded successfully");
                0
            }
            "read" => {
                if subfunction_args.len() < 13 {
                    display("Error: read requires at least 13 arguments");
                    return 198;
                }

                if !read::data_exists(subfunction_args[0]) {
                    display(&format!("File does not exist ({})", subfunction_args[0]));
                    return 601;
                }

                let parallel_strategy: Option<ParallelizationStrategy> = match subfunction_args[6] {
                    "columns" => Some(ParallelizationStrategy::ByColumn),
                    "rows" => Some(ParallelizationStrategy::ByRow),
                    _ => None,
                };

                let safe_relaxed = subfunction_args[7] == "1";
                let asterisk_to_variable_name = if subfunction_args[8].is_empty() {
                    None
                } else {
                    Some(subfunction_args[8])
                };

                let batch_size = if subfunction_args.len() >= 14 {
                    subfunction_args[13].parse::<usize>().unwrap_or(50_000)
                } else {
                    50_000
                };

                let read_result = read::read_to_stata(
                    subfunction_args[0],
                    subfunction_args[1],
                    subfunction_args[2].parse::<usize>().unwrap_or(0),
                    subfunction_args[3].parse::<usize>().unwrap_or(0),
                    Some(subfunction_args[4]),
                    subfunction_args[5],
                    parallel_strategy,
                    safe_relaxed,
                    asterisk_to_variable_name,
                    subfunction_args[9],
                    subfunction_args[10].parse::<usize>().unwrap_or(0),
                    subfunction_args[11].parse::<f64>().unwrap_or(0.0),
                    subfunction_args[12].parse::<u64>().unwrap_or(0),
                    batch_size,
                );

                match read_result {
                    Ok(code) => code,
                    Err(e) => {
                        display(&format!("Error reading the file = {:?}", e));
                        198
                    }
                }
            }
            "save" => {
                if subfunction_args.len() < 12 {
                    display("Error: save requires 12 arguments");
                    return 198;
                }

                let compression_level_raw = subfunction_args[8].parse::<isize>().unwrap_or(-1);
                let compression_level = if compression_level_raw < 0 {
                    None
                } else {
                    Some(compression_level_raw as usize)
                };

                let save_result = write::write_from_stata(
                    subfunction_args[0],
                    subfunction_args[1],
                    subfunction_args[2].parse::<usize>().unwrap_or(0),
                    subfunction_args[3].parse::<usize>().unwrap_or(0),
                    Some(subfunction_args[4]),
                    subfunction_args[5],
                    None,
                    subfunction_args[6],
                    subfunction_args[7],
                    compression_level,
                    subfunction_args[9] == "1",
                    subfunction_args[10] == "1",
                    subfunction_args[11] == "1",
                );

                match save_result {
                    Ok(code) => code,
                    Err(e) => {
                        display(&format!("Error writing parquet file = {:?}", e));
                        198
                    }
                }
            }
            "describe" => {
                if subfunction_args.len() < 7 {
                    display("Error: describe requires 7 arguments");
                    return 198;
                }

                if !read::data_exists(subfunction_args[0]) {
                    display(&format!("File does not exist ({})", subfunction_args[0]));
                    return 601;
                }

                let asterisk_to_variable_name = if subfunction_args[4].is_empty() {
                    None
                } else {
                    Some(subfunction_args[4])
                };

                read::file_summary(
                    subfunction_args[0],
                    subfunction_args[1] == "1",
                    subfunction_args[2] == "1",
                    Some(subfunction_args[3]),
                    true,
                    asterisk_to_variable_name,
                    subfunction_args[5] == "1",
                    subfunction_args[6] == "1",
                )
            }
            "has_metadata_key" => {
                if subfunction_args.len() < 2 {
                    display("Error: has_metadata_key requires 2 arguments");
                    return 198;
                }

                if !read::data_exists(subfunction_args[0]) {
                    display(&format!("File does not exist ({})", subfunction_args[0]));
                    return 601;
                }

                match read::has_metadata_key(subfunction_args[0], subfunction_args[1]) {
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
            _ => {
                display(&format!(
                    "Error: Unknown subfunction '{}'",
                    subfunction_name
                ));
                198
            }
        }
    })
    .unwrap_or_else(|_| {
        display("Panic occurred in dtparquet plugin");
        198
    })
}
