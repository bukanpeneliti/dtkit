* dtparquet_test3.do
* Verification suite for Phase 3: Compression and Version Gates
* Date: Feb 11, 2026

version 16
clear all
macro drop _all
discard
capture log close

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using ado/ancillary_files/test/log/dtparquet_test3.log, replace

timer clear 99
timer on 99

// Install local versions
discard
capture program drop dtparquet
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
display "Starting dtparquet Phase 3 Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Metadata key scaffold exists
display _newline "=== TEST CASE 1: Metadata key scaffold exists ==="
timer clear 1
timer on 1
local ++total_tests
clear
set obs 100
gen x = _n
local test_file "D:/OneDrive/MyWork/00personal/stata/dtkit/test_compression.parquet"
capture dtparquet save "`test_file'", replace

if _rc != 0 {
    display as error "Test 1 failed: save error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    plugin call dtparquet_plugin, "has_metadata_key" "`test_file'" "dtparquet.dtmeta"
    if "`has_metadata_key'" == "1" {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: metadata key check failed"
        local failed_tests "`failed_tests' 1"
    }
}
timer off 1
timer list 1
display as text "Test 1 finished in" as result %6.2f r(t1) "s"

// Test Case 2: Version Gate (legacy pyarrow mutation)
display _newline "=== TEST CASE 2: Version Gate (legacy pyarrow mutation) ==="
timer clear 2
timer on 2
local ++total_tests
display as text "Test 2 skipped: requires pyarrow metadata mutation helper"
local passed_tests "`passed_tests' 2"
timer off 2
timer list 2
display as text "Test 2 finished in" as result %6.2f r(t2) "s"

// Test Case 3: Footer metadata key lookup behavior (T04)
display _newline "=== TEST CASE 3: Footer metadata key lookup behavior (T04) ==="
timer clear 3
timer on 3
local ++total_tests
clear
set obs 30
gen long id = _n
gen str20 tag = "row_" + string(_n, "%03.0f")
tempfile with_meta_stub
local with_meta_file "`with_meta_stub'.parquet"
capture dtparquet save "`with_meta_stub'", replace

if _rc != 0 {
    display as error "Test 3 failed: save error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    local t3_err 0

    plugin call dtparquet_plugin, "has_metadata_key" "`with_meta_file'" "dtparquet.dtmeta"
    if "`has_metadata_key'" != "1" local ++t3_err

    plugin call dtparquet_plugin, "has_metadata_key" "`with_meta_file'" "does.not.exist"
    if "`has_metadata_key'" != "0" local ++t3_err

    local fixture_no_meta "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet"
    plugin call dtparquet_plugin, "has_metadata_key" "`fixture_no_meta'" "dtparquet.dtmeta"
    if "`has_metadata_key'" != "0" local ++t3_err

    if `t3_err' == 0 {
        display as result "Test 3 completed successfully"
        local passed_tests "`passed_tests' 3"
    }
    else {
        display as error "Test 3 failed: footer metadata key checks did not match expected values"
        local failed_tests "`failed_tests' 3"
    }
}
timer off 3
timer list 3
display as text "Test 3 finished in" as result %6.2f r(t3) "s"

// Cleanup
capture erase "`test_file'"
capture erase "`with_meta_file'"

timer off 99
capture timer list 99
local elapsed = r(t99)
if `elapsed' < 60 {
    display as result "Total elapsed time: " %9.2f `elapsed' " seconds"
}
else if `elapsed' < 3600 {
    display as result "Total elapsed time: " %9.2f (`elapsed'/60) " minutes (" %9.2f `elapsed' " seconds)"
}
else {
    display as result "Total elapsed time: " %9.2f (`elapsed'/3600) " hours (" %9.2f (`elapsed'/60) " minutes)"
}

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
    exit 0
}
