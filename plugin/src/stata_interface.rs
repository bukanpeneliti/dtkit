#![allow(clippy::result_unit_err)]

use std::sync::atomic::{AtomicU64, Ordering};

pub use stata_sys::{
    display, set_macro, SF_error, SF_is_missing, SF_nobs, SF_nvar, SF_sdata, SF_sdatalen,
    SF_strldata,
};

#[allow(non_camel_case_types)]
pub type ST_retcode = i32;

static REPLACE_NUMBER_CALLS: AtomicU64 = AtomicU64::new(0);
static REPLACE_STRING_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_NUMERIC_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_STRING_CALLS: AtomicU64 = AtomicU64::new(0);
static PULL_STRL_CALLS: AtomicU64 = AtomicU64::new(0);
static STRL_TRUNC_EVENTS: AtomicU64 = AtomicU64::new(0);
static STRL_BINARY_EVENTS: AtomicU64 = AtomicU64::new(0);
static TRANSFER_FALLBACK_CALLS: AtomicU64 = AtomicU64::new(0);
static TRANSFER_CONVERSION_FAILURES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct TransferMetrics {
    pub replace_number_calls: u64,
    pub replace_string_calls: u64,
    pub pull_numeric_calls: u64,
    pub pull_string_calls: u64,
    pub pull_strl_calls: u64,
    pub strl_trunc_events: u64,
    pub strl_binary_events: u64,
    pub fallback_calls: u64,
    pub conversion_failures: u64,
}

pub fn reset_transfer_metrics() {
    REPLACE_NUMBER_CALLS.store(0, Ordering::Relaxed);
    REPLACE_STRING_CALLS.store(0, Ordering::Relaxed);
    PULL_NUMERIC_CALLS.store(0, Ordering::Relaxed);
    PULL_STRING_CALLS.store(0, Ordering::Relaxed);
    PULL_STRL_CALLS.store(0, Ordering::Relaxed);
    STRL_TRUNC_EVENTS.store(0, Ordering::Relaxed);
    STRL_BINARY_EVENTS.store(0, Ordering::Relaxed);
    TRANSFER_FALLBACK_CALLS.store(0, Ordering::Relaxed);
    TRANSFER_CONVERSION_FAILURES.store(0, Ordering::Relaxed);
}

pub fn read_transfer_metrics() -> TransferMetrics {
    TransferMetrics {
        replace_number_calls: REPLACE_NUMBER_CALLS.load(Ordering::Relaxed),
        replace_string_calls: REPLACE_STRING_CALLS.load(Ordering::Relaxed),
        pull_numeric_calls: PULL_NUMERIC_CALLS.load(Ordering::Relaxed),
        pull_string_calls: PULL_STRING_CALLS.load(Ordering::Relaxed),
        pull_strl_calls: PULL_STRL_CALLS.load(Ordering::Relaxed),
        strl_trunc_events: STRL_TRUNC_EVENTS.load(Ordering::Relaxed),
        strl_binary_events: STRL_BINARY_EVENTS.load(Ordering::Relaxed),
        fallback_calls: TRANSFER_FALLBACK_CALLS.load(Ordering::Relaxed),
        conversion_failures: TRANSFER_CONVERSION_FAILURES.load(Ordering::Relaxed),
    }
}

pub fn publish_transfer_metrics(prefix: &str) {
    let metrics = read_transfer_metrics();
    let total_calls = metrics.replace_number_calls
        + metrics.replace_string_calls
        + metrics.pull_numeric_calls
        + metrics.pull_string_calls
        + metrics.pull_strl_calls;

    set_macro(
        &format!("{}_replace_number_calls", prefix),
        &metrics.replace_number_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_replace_string_calls", prefix),
        &metrics.replace_string_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_pull_numeric_calls", prefix),
        &metrics.pull_numeric_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_pull_string_calls", prefix),
        &metrics.pull_string_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_pull_strl_calls", prefix),
        &metrics.pull_strl_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_strl_trunc_events", prefix),
        &metrics.strl_trunc_events.to_string(),
        true,
    );
    set_macro(
        &format!("{}_strl_binary_events", prefix),
        &metrics.strl_binary_events.to_string(),
        true,
    );
    set_macro(
        &format!("{}_transfer_calls_total", prefix),
        &total_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_fallback_calls", prefix),
        &metrics.fallback_calls.to_string(),
        true,
    );
    set_macro(
        &format!("{}_conversion_failures", prefix),
        &metrics.conversion_failures.to_string(),
        true,
    );
}

pub fn read_macro(macro_name: &str, global: bool, buffer_size: Option<usize>) -> String {
    stata_sys::get_macro(macro_name, global, buffer_size).unwrap_or_default()
}

pub fn replace_number(value: Option<f64>, row: usize, column: usize) -> i32 {
    REPLACE_NUMBER_CALLS.fetch_add(1, Ordering::Relaxed);
    stata_sys::replace_number(value, row, column)
}

pub fn replace_string(value: Option<String>, row: usize, column: usize) -> i32 {
    REPLACE_STRING_CALLS.fetch_add(1, Ordering::Relaxed);
    stata_sys::replace_string(value, row, column)
}

pub fn record_transfer_fallback() {
    TRANSFER_FALLBACK_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_transfer_conversion_failure() {
    TRANSFER_CONVERSION_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub fn count_rows() -> i32 {
    unsafe { SF_nobs() }
}

pub fn count_vars() -> i32 {
    unsafe { SF_nvar() }
}

pub fn pull_numeric_cell(col: usize, row: usize) -> Option<f64> {
    PULL_NUMERIC_CALLS.fetch_add(1, Ordering::Relaxed);
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

pub fn pull_string_cell(col: usize, row: usize, max_len: usize) -> String {
    let mut buffer: Vec<i8> = vec![0; max_len + 1];
    pull_string_cell_with_buffer(col, row, &mut buffer)
}

pub fn pull_string_cell_with_buffer(col: usize, row: usize, buffer: &mut Vec<i8>) -> String {
    use std::ffi::{c_char, CStr};
    PULL_STRING_CALLS.fetch_add(1, Ordering::Relaxed);

    unsafe {
        if !buffer.is_empty() {
            buffer[0] = 0;
        }
        SF_sdata(col as i32, row as i32, buffer.as_mut_ptr() as *mut c_char);

        CStr::from_ptr(buffer.as_ptr() as *const c_char)
            .to_string_lossy()
            .into_owned()
    }
}

#[derive(Debug, Default)]
pub struct StrlArena {
    buffer: Vec<u8>,
}

impl StrlArena {
    const CHUNK_BYTES: usize = 16 * 1024;

    pub fn new() -> Self {
        Self::default()
    }

    fn reserve_for_len(&mut self, len: usize) {
        if self.buffer.len() >= len {
            return;
        }
        let rounded = len.div_ceil(Self::CHUNK_BYTES) * Self::CHUNK_BYTES;
        self.buffer.resize(rounded, 0);
    }
}

pub fn pull_strl_cell(col: usize, row: usize) -> Result<String, ()> {
    let mut arena = StrlArena::new();
    pull_strl_cell_with_arena(col, row, &mut arena)
}

pub fn pull_strl_cell_with_arena(
    col: usize,
    row: usize,
    arena: &mut StrlArena,
) -> Result<String, ()> {
    use std::ffi::c_char;
    PULL_STRL_CALLS.fetch_add(1, Ordering::Relaxed);
    unsafe {
        let len = SF_sdatalen(col as i32, row as i32);
        if len < 0 {
            return Err(());
        }

        let read_len = len.checked_add(1).ok_or(())?;
        let len_usize = len as usize;
        arena.reserve_for_len(len_usize.saturating_add(1));
        let buffer = &mut arena.buffer[..len_usize.saturating_add(1)];
        SF_strldata(
            col as i32,
            row as i32,
            buffer.as_mut_ptr() as *mut c_char,
            read_len,
        );

        let end = buffer[..len_usize]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(len_usize);
        if end < len_usize {
            STRL_TRUNC_EVENTS.fetch_add(1, Ordering::Relaxed);
        }

        let bytes = &buffer[..end];
        match std::str::from_utf8(bytes) {
            Ok(text) => Ok(text.to_owned()),
            Err(_) => {
                STRL_BINARY_EVENTS.fetch_add(1, Ordering::Relaxed);
                Ok(String::from_utf8_lossy(bytes).into_owned())
            }
        }
    }
}

pub fn error(msg: &str) {
    use std::ffi::CString;
    let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("Error").unwrap());
    unsafe {
        SF_error(c_msg.as_ptr() as *mut std::os::raw::c_char);
    }
}
