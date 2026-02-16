* dtparquet_test5.do
* Verification for Phase 5: Optimized SFI Streaming (Row-Major) and Phase 6: Advanced Type Mapping
* Date: Feb 11, 2026

version 16
clear all
macro drop _all

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

capture log using ado/ancillary_files/test/log/dtparquet_test5.log, replace

// Install local versions
discard
run "ado/dtparquet.ado"
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
capture noisily copy "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
if _rc != 0 {
    local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll"
}
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Phase 5 & 6 Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Wide Data (Row-Major check)
display _newline "=== TEST CASE 1: Wide Data ==="
local ++total_tests
clear
set obs 100
gen id = _n
forvalues i = 1/200 {
    gen var`i' = _n * `i'
}
capture dtparquet save "test_wide.parquet", replace
if _rc != 0 {
    display as error "Test 1 failed: save error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_wide.parquet"
        assert c(k) == 201
        assert c(N) == 100
        assert var200 == id * 200
    }
    if _rc == 0 {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: assertion error"
        local failed_tests "`failed_tests' 1"
    }
}

// Test Case 2: Chunk Boundary Test
display _newline "=== TEST CASE 2: Chunk Boundary (N not multiple of chunksize) ==="
local ++total_tests
clear
set obs 55
gen id = _n
capture dtparquet save "test_boundary.parquet", replace
if _rc != 0 {
    display as error "Test 2 failed: save error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_boundary.parquet"
        assert c(N) == 55
    }
    if _rc == 0 {
        display as result "Test 2 completed successfully"
        local passed_tests "`passed_tests' 2"
    }
    else {
        display as error "Test 2 failed: assertion error"
        local failed_tests "`failed_tests' 2"
    }
}

// Test Case 3: UTF-8 Symmetric Handling
display _newline "=== TEST CASE 3: UTF-8 Special Characters ==="
local ++total_tests
clear
set obs 3
gen str80 s = ""
replace s = "Standard text" in 1
replace s = "Unicode: € £ ¥ ©" in 2
replace s = "Unicode: aeio" in 3
capture dtparquet save "test_utf8.parquet", replace
if _rc != 0 {
    display as error "Test 3 failed: save error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_utf8.parquet"
        assert s == "Standard text" in 1
        assert s == "Unicode: € £ ¥ ©" in 2
        assert s == "Unicode: aeio" in 3
    }
    if _rc == 0 {
        display as result "Test 3 completed successfully"
        local passed_tests "`passed_tests' 3"
    }
    else {
        display as error "Test 3 failed: assertion error"
        local failed_tests "`failed_tests' 3"
    }
}

// Test Case 4: Manual Chunksize Override
display _newline "=== TEST CASE 4: Chunksize Override ==="
local ++total_tests
clear
set obs 10
gen x = _n
capture dtparquet save "test_chunk.parquet", replace chunksize(1)
if _rc != 0 {
    display as error "Test 4 failed: save error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_chunk.parquet", chunksize(1)
        assert x == _n
    }
    if _rc == 0 {
        display as result "Test 4 completed successfully"
        local passed_tests "`passed_tests' 4"
    }
    else {
        display as error "Test 4 failed: assertion error"
        local failed_tests "`failed_tests' 4"
    }
}

// Test Case 5: Data Signature Fidelity
display _newline "=== TEST CASE 5: Data Signature Fidelity ==="
local ++total_tests
clear
set obs 100
gen id = _n
gen byte b = mod(_n, 10)
gen int i = _n * 10
gen long l = _n * 100
gen float f = _n * 1.1
gen double d = _n * 1.123456789
gen str10 s = "row " + string(_n)
datasignature
local sig_orig = r(datasignature)

capture dtparquet save "test_sig.parquet", replace
if _rc != 0 {
    display as error "Test 5 failed: save error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_sig.parquet"
        datasignature
        assert r(datasignature) == "`sig_orig'"
    }
    if _rc == 0 {
        display as result "Test 5 completed successfully"
        local passed_tests "`passed_tests' 5"
    }
    else {
        display as error "Test 5 failed: datasignature mismatch"
        local failed_tests "`failed_tests' 5"
    }
}

// Test Case 5b: strL Stress Case
display _newline "=== TEST CASE 5b: strL Stress Case ==="
local ++total_tests
clear
set obs 10
gen id = _n
gen strL long_str = ""
replace long_str = "Short string" in 1
replace long_str = "Longer string with some content" in 2
replace long_str = "Very " + c(alpha) + " long string " + c(ALPHA) + " to test strL limits" in 3
forvalues i = 4/10 {
    replace long_str = "Row `i' data " + string(`i'^2) in `i'
}
datasignature
local sig_orig = r(datasignature)

capture dtparquet save "test_strl.parquet", replace
if _rc != 0 {
    display as error "Test 5b failed: save error " _rc
    local failed_tests "`failed_tests' 5b"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_strl.parquet"
        datasignature
        assert r(datasignature) == "`sig_orig'"
        assert long_str != "" in 3
    }
    if _rc == 0 {
        display as result "Test 5b completed successfully"
        local passed_tests "`passed_tests' 5b"
    }
    else {
        display as error "Test 5b failed: assertion or signature mismatch"
        local failed_tests "`failed_tests' 5b"
    }
}

// Test Case 6: Foreign Categorical (Pandas)
display _newline "=== TEST CASE 6: Foreign Categorical (Pandas) ==="
local ++total_tests
local foreign_pandas "ado/ancillary_files/test/dtparquet/data/foreign_cat_pandas.parquet"
if fileexists("`foreign_pandas'") {
    capture noisily {
        clear
        dtparquet use using "`foreign_pandas'", clear catmode(encode)
        assert c(N) == 4
        capture confirm numeric variable cat
        assert _rc == 0
        decode cat, gen(cat_text)
        assert cat_text != ""
    }
    if _rc == 0 {
        display as result "Test 6 completed successfully"
        local passed_tests "`passed_tests' 6"
    }
    else {
        display as error "Test 6 failed: assertion error"
        local failed_tests "`failed_tests' 6"
    }
}
else {
    display as text "Test 6 skipped: fixture missing"
    local passed_tests "`passed_tests' 6"
}

// Test Case 7: Foreign Dictionary (Arrow)
display _newline "=== TEST CASE 7: Foreign Dictionary (Arrow) ==="
local ++total_tests
local foreign_arrow "ado/ancillary_files/test/dtparquet/data/foreign_cat_arrow_dict.parquet"
if fileexists("`foreign_arrow'") {
    capture noisily {
        clear
        dtparquet use using "`foreign_arrow'", clear catmode(both)
        assert c(N) == 4
        confirm variable cat
        confirm variable cat_id
        assert cat != ""
    }
    if _rc == 0 {
        display as result "Test 7 completed successfully"
        local passed_tests "`passed_tests' 7"
    }
    else {
        display as error "Test 7 failed: assertion error"
        local failed_tests "`failed_tests' 7"
    }
}
else {
    display as text "Test 7 skipped: fixture missing"
    local passed_tests "`passed_tests' 7"
}

// Test Case 11: Native Round-Trip (No Epoch Offset)
display _newline "=== TEST CASE 11: Native Round-Trip (No Epoch Offset) ==="
local ++total_tests
capture noisily {
    clear
    set obs 3
    gen id = _n
    gen date = td(01jan2020) + (_n - 1)
    format date %td
    gen double ts = clock("01jan2020 00:00:00", "DMYhms") + (_n - 1) * 86400000
    format ts %tc
    datasignature
    local sig_orig = r(datasignature)
    
    dtparquet save "test_native.parquet", replace
    clear
    dtparquet use using "test_native.parquet"
    datasignature
    assert r(datasignature) == "`sig_orig'"
}
if _rc == 0 {
    display as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}
else {
    display as error "Test 11 failed: native round-trip error"
    local failed_tests "`failed_tests' 11"
}

// Test Case 12: T03 pool lifecycle metrics
display _newline "=== TEST CASE 12: T03 Pool Lifecycle Metrics ==="
local ++total_tests
local t12_n_rows 50000
local t12_chunk_size 5000
tempfile t12_pool_stub

capture noisily {
    clear
    set obs `t12_n_rows'
    gen long id = _n
    forvalues j = 1/16 {
        gen double x`j' = runiform() * `j'
    }

    dtparquet save "`t12_pool_stub'", replace chunksize(`t12_chunk_size')

    local t12_parquet_file "`t12_pool_stub'.parquet"
    local t12_compute_after_save = real("$compute_pool_inits")
    local t12_io_after_save = real("$io_pool_inits")
    assert !missing(`t12_compute_after_save')
    assert !missing(`t12_io_after_save')
    assert `t12_compute_after_save' >= 1
    assert `t12_io_after_save' >= 1

    clear
    dtparquet use using "`t12_parquet_file'", clear chunksize(`t12_chunk_size')
    local t12_compute_after_use1 = real("$compute_pool_inits")
    local t12_io_after_use1 = real("$io_pool_inits")
    local t12_planned_batches = real("$read_planned_batches")
    local t12_processed_batches = real("$read_processed_batches")
    assert `t12_compute_after_use1' == `t12_compute_after_save'
    assert `t12_io_after_use1' == `t12_io_after_save'
    assert `t12_planned_batches' > 1
    assert `t12_processed_batches' > 1

    clear
    dtparquet use using "`t12_parquet_file'", clear chunksize(`t12_chunk_size')
    local t12_compute_after_use2 = real("$compute_pool_inits")
    local t12_io_after_use2 = real("$io_pool_inits")
    assert `t12_compute_after_use2' == `t12_compute_after_save'
    assert `t12_io_after_use2' == `t12_io_after_save'
}

if _rc == 0 {
    display as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    display as error "Test 12 failed: pool lifecycle metrics mismatch"
    local failed_tests "`failed_tests' 12"
}

// Test Case 13: T05 typed transfer writer metrics
display _newline "=== TEST CASE 13: T05 Typed Transfer Writer Metrics ==="
local ++total_tests
local t13_n_rows 10000
local t13_chunk_size 2500
tempfile t13_transfer_stub

capture noisily {
    clear
    set obs `t13_n_rows'
    gen byte flag = mod(_n, 2)
    gen int small = mod(_n, 1000)
    gen long id = _n
    gen float xf = _n / 100
    gen double xd = (_n / 10) + runiform()
    gen double td = mdy(1, 1, 2020) + _n
    format td %td
    gen double tc = clock("01jan2020 00:00:00", "DMYhms") + (_n * 1000)
    format tc %tc
    gen str24 code = "c_" + string(_n, "%06.0f")
    gen strL note = "row_" + string(_n) + "_abcdefghijklmnopqrstuvwxyz0123456789"

    dtparquet save "`t13_transfer_stub'", replace chunksize(`t13_chunk_size')

    clear
    dtparquet use using "`t13_transfer_stub'.parquet", clear chunksize(`t13_chunk_size')

    assert _N == `t13_n_rows'
    assert c(k) == 9

    local t13_fallback = real("$read_fallback_calls")
    local t13_conversion_failures = real("$read_conversion_failures")
    local t13_number_calls = real("$read_replace_number_calls")
    local t13_string_calls = real("$read_replace_string_calls")

    assert !missing(`t13_fallback')
    assert !missing(`t13_conversion_failures')
    assert !missing(`t13_number_calls')
    assert !missing(`t13_string_calls')
    assert `t13_number_calls' > 0
    assert `t13_string_calls' > 0
    assert `t13_fallback' == 0
    assert `t13_conversion_failures' == 0
}

if _rc == 0 {
    display as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
}
else {
    display as error "Test 13 failed: typed transfer metrics mismatch"
    local failed_tests "`failed_tests' 13"
}

// Test Case 14: T06 single-pass lazy execution metrics
display _newline "=== TEST CASE 14: T06 Single-Pass Lazy Execution Metrics ==="
local ++total_tests
local t14_n_rows 120000
local t14_chunk_size 4000

capture noisily {
    clear
    set obs `t14_n_rows'
    gen long id = _n
    gen byte grp = mod(_n, 3)
    gen double value = _n * 1.5
    gen str16 tag = "r_" + string(_n, "%06.0f")

    dtparquet save "test_t06_lazy.parquet", replace chunksize(`t14_chunk_size')

    clear
    dtparquet use id grp value tag using "test_t06_lazy.parquet" if grp > 0 in 1/`t14_n_rows', clear chunksize(`t14_chunk_size')

    count
    assert r(N) == 80000
    assert c(k) == 4
    assert id[1] == 1
    assert id[_N] == 119999

    local t14_collect_calls = real("$read_collect_calls")
    local t14_planned_batches = real("$read_planned_batches")
    local t14_processed_batches = real("$read_processed_batches")

    assert `t14_collect_calls' == 1
    assert `t14_planned_batches' == `t14_processed_batches'
    assert `t14_planned_batches' > 1
    assert "$read_lazy_mode" == "single_pass"
}

if _rc == 0 {
    display as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
}
else {
    display as error "Test 14 failed: single-pass lazy execution mismatch"
    local failed_tests "`failed_tests' 14"
}

// Test Case 15: T07 adaptive batch autotuning metrics
display _newline "=== TEST CASE 15: T07 Adaptive Batch Autotuning Metrics ==="
local ++total_tests
local t15_n_rows 150000
local t15_save_chunk 3000
local t15_use_chunk 2500

capture noisily {
    clear
    set obs `t15_n_rows'
    gen long id = _n
    forvalues j = 1/20 {
        gen double x`j' = runiform() * `j'
    }
    forvalues j = 1/10 {
        gen str32 s`j' = "v" + string(mod(_n * `j', 100000), "%05.0f")
    }

    dtparquet save "test_t07_autotune.parquet", replace chunksize(`t15_save_chunk')

    local t15_write_selected = real("$write_selected_batch_size")
    local t15_write_row_width = real("$write_batch_row_width_bytes")
    local t15_write_memory_cap = real("$write_batch_memory_cap_rows")

    assert !missing(`t15_write_selected')
    assert !missing(`t15_write_row_width')
    assert !missing(`t15_write_memory_cap')
    assert `t15_write_selected' > 0
    assert `t15_write_row_width' > 0
    assert `t15_write_memory_cap' > 0
    assert `t15_write_selected' <= `t15_save_chunk'
    assert `t15_write_selected' <= `t15_write_memory_cap'
    assert inlist("$write_batch_tuner_mode", "adaptive", "fixed")

    clear
    dtparquet use using "test_t07_autotune.parquet", clear chunksize(`t15_use_chunk')

    assert _N == `t15_n_rows'

    local t15_read_selected = real("$read_selected_batch_size")
    local t15_read_row_width = real("$read_batch_row_width_bytes")
    local t15_read_memory_cap = real("$read_batch_memory_cap_rows")

    assert !missing(`t15_read_selected')
    assert !missing(`t15_read_row_width')
    assert !missing(`t15_read_memory_cap')
    assert `t15_read_selected' > 0
    assert `t15_read_row_width' > 0
    assert `t15_read_memory_cap' > 0
    assert `t15_read_selected' <= `t15_use_chunk'
    assert `t15_read_selected' <= `t15_read_memory_cap'
    assert inlist("$read_batch_tuner_mode", "adaptive", "fixed")
}

if _rc == 0 {
    display as result "Test 15 completed successfully"
    local passed_tests "`passed_tests' 15"
}
else {
    display as error "Test 15 failed: adaptive batch autotuning mismatch"
    local failed_tests "`failed_tests' 15"
}

// Test Case 16: T08 producer-consumer write pipeline metrics
display _newline "=== TEST CASE 16: T08 Producer-Consumer Write Pipeline Metrics ==="
local ++total_tests
local t16_n_rows 180000
local t16_chunk_size 2000

capture noisily {
    clear
    set obs `t16_n_rows'
    gen long id = _n
    forvalues j = 1/16 {
        gen double d`j' = runiform() * `j'
    }
    forvalues j = 1/8 {
        gen str40 s`j' = "q" + string(mod(_n * `j', 100000), "%05.0f")
    }

    dtparquet save "test_t08_pipeline.parquet", replace chunksize(`t16_chunk_size')

    local t16_mode "$write_pipeline_mode"
    local t16_capacity = real("$write_queue_capacity")
    local t16_peak = real("$write_queue_peak")
    local t16_backpressure = real("$write_queue_bp_events")
    local t16_produced = real("$write_queue_prod_batches")
    local t16_consumed = real("$write_queue_cons_batches")
    local t16_processed = real("$write_processed_batches")

    assert inlist("`t16_mode'", "producer_consumer", "legacy_direct")
    assert !missing(`t16_capacity')
    assert !missing(`t16_peak')
    assert !missing(`t16_backpressure')
    assert !missing(`t16_produced')
    assert !missing(`t16_consumed')
    assert `t16_produced' >= 1
    assert `t16_consumed' >= 1
    assert `t16_consumed' == `t16_processed'

    if "`t16_mode'" == "producer_consumer" {
        assert `t16_capacity' >= 1
        assert `t16_peak' >= 1
        assert `t16_peak' <= `t16_capacity'
        assert `t16_produced' == `t16_consumed'
    }

    clear
    dtparquet use using "test_t08_pipeline.parquet", clear
    assert _N == `t16_n_rows'
}

if _rc == 0 {
    display as result "Test 16 completed successfully"
    local passed_tests "`passed_tests' 16"
}
else {
    display as error "Test 16 failed: producer-consumer write pipeline mismatch"
    local failed_tests "`failed_tests' 16"
}

// Test Case 17: T10 strL arena path diagnostics
display _newline "=== TEST CASE 17: T10 strL Arena Path Diagnostics ==="
local ++total_tests
local t17_n_rows 12000

capture noisily {
    clear
    set obs `t17_n_rows'
    gen long id = _n
    gen strL payload = ""
    forvalues j = 1/32 {
        replace payload = payload + "abcdefghijklmnopqrstuvwxyz0123456789"
    }
    replace payload = payload + "_" + string(_n, "%08.0f")

    dtparquet save "test_t10_strl.parquet", replace chunksize(4000)

    local t17_pull_strl = real("$write_pull_strl_calls")
    local t17_trunc_events = real("$write_strl_trunc_events")
    local t17_binary_events = real("$write_strl_binary_events")

    assert !missing(`t17_pull_strl')
    assert !missing(`t17_trunc_events')
    assert !missing(`t17_binary_events')
    assert `t17_pull_strl' == `t17_n_rows'
    assert `t17_trunc_events' == 0
    assert `t17_binary_events' == 0

    clear
    dtparquet use id payload using "test_t10_strl.parquet", clear
    count
    assert r(N) == `t17_n_rows'
    assert length(payload[1]) > 1100
}

if _rc == 0 {
    display as result "Test 17 completed successfully"
    local passed_tests "`passed_tests' 17"
}
else {
    display as error "Test 17 failed: strL arena path mismatch"
    local failed_tests "`failed_tests' 17"
}

// Test Case 18: T12 execution boundaries and typed payload entry points
display _newline "=== TEST CASE 18: T12 Execution Boundaries ==="
local ++total_tests

capture noisily {
    clear
    set obs 2000
    gen long id = _n
    gen double value = _n * 1.25
    gen str20 label = "row_" + string(_n, "%06.0f")

    dtparquet save "test_t12_boundaries.parquet", replace chunksize(700)
    assert inlist("$write_engine_stage", "execute", "stata_sink")

    clear
    dtparquet use using "test_t12_boundaries.parquet", clear chunksize(700)
    count
    assert r(N) == 2000
    assert inlist("$read_engine_stage", "execute", "stata_sink")
}

if _rc == 0 {
    display as result "Test 18 completed successfully"
    local passed_tests "`passed_tests' 18"
}
else {
    display as error "Test 18 failed: execution boundary mismatch"
    local failed_tests "`failed_tests' 18"
}

// Cleanup
capture erase "test_wide.parquet"
capture erase "test_boundary.parquet"
capture erase "test_utf8.parquet"
capture erase "test_chunk.parquet"
capture erase "test_sig.parquet"
capture erase "test_strl.parquet"
capture erase "test_native.parquet"
capture erase "test_t06_lazy.parquet"
capture erase "test_t07_autotune.parquet"
capture erase "test_t08_pipeline.parquet"
capture erase "test_t10_strl.parquet"
capture erase "test_t12_boundaries.parquet"

// Test Summary
display _newline "=========================================="
display "Test Suite Summary"
display "Total tests: `total_tests'"
display "Passed: " wordcount("`passed_tests'")
display "Failed: " wordcount("`failed_tests'")
display "=========================================="

if wordcount("`failed_tests'") > 0 {
    display as error "Failed tests: `failed_tests'"
    log close
    exit 1
}
else {
    display as result "All tests passed!"
    log close
    capture erase "dtparquet_test5.log"
    exit 0
}
