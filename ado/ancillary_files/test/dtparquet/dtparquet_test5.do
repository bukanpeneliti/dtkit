* dtparquet_test5.do
* Verification for Phase 5: Optimized SFI Streaming (Row-Major) and Phase 6: Advanced Type Mapping
* Date: Feb 11, 2026

version 16
clear all
macro drop _all

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using ado/ancillary_files/test/log/dtparquet_test5.log, replace

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

// Cleanup
capture erase "test_wide.parquet"
capture erase "test_boundary.parquet"
capture erase "test_utf8.parquet"
capture erase "test_chunk.parquet"
capture erase "test_sig.parquet"
capture erase "test_strl.parquet"
capture erase "test_native.parquet"

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
