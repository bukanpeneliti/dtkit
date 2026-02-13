pub use stata_sys::{
    display, replace_number, replace_string, set_macro, SF_error, SF_is_missing, SF_nobs, SF_nvar,
    SF_sdata, SF_sdatalen, SF_strldata, SF_var_is_binary,
};

#[allow(non_camel_case_types)]
pub type ST_retcode = i32;

pub fn get_macro(macro_name: &str, global: bool, buffer_size: Option<usize>) -> String {
    stata_sys::get_macro(macro_name, global, buffer_size).unwrap_or_default()
}

pub fn n_obs() -> i32 {
    unsafe { SF_nobs() }
}

pub fn n_var() -> i32 {
    unsafe { SF_nvar() }
}

pub fn read_numeric(col: usize, row: usize) -> Option<f64> {
    let mut value: f64 = 0.0;
    unsafe {
        let result = stata_sys::SF_vdata(col as i32, row as i32, &mut value);
        if result != 0 || SF_is_missing(value) {
            None
        } else {
            Some(value)
        }
    }
}

pub fn read_string(col: usize, row: usize, max_len: usize) -> String {
    use std::ffi::{c_char, CStr};
    let mut buffer: Vec<i8> = vec![0; max_len + 1];

    unsafe {
        SF_sdata(col as i32, row as i32, buffer.as_mut_ptr() as *mut c_char);

        CStr::from_ptr(buffer.as_ptr() as *const c_char)
            .to_string_lossy()
            .into_owned()
    }
}

pub fn read_string_strl(col: usize, row: usize) -> Result<String, ()> {
    use std::ffi::c_char;
    unsafe {
        let len = SF_sdatalen(col as i32, row as i32);
        if len < 0 {
            return Err(());
        }

        let len_usize = len as usize;
        let mut buffer: Vec<u8> = vec![0; len_usize.saturating_add(1)];
        SF_strldata(
            col as i32,
            row as i32,
            buffer.as_mut_ptr() as *mut c_char,
            len + 1,
        );

        let end = buffer.iter().position(|&b| b == 0).unwrap_or(len_usize);
        Ok(String::from_utf8_lossy(&buffer[..end]).into_owned())
    }
}

pub fn error(msg: &str) {
    use std::ffi::CString;
    let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("Error").unwrap());
    unsafe {
        SF_error(c_msg.as_ptr() as *mut std::os::raw::c_char);
    }
}
