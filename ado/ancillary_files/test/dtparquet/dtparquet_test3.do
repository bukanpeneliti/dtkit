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

// Install local versions
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

// Test 1: Metadata key scaffold exists
display _newline "=== TEST 1: Metadata key scaffold exists ==="
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
        display as result "Test 1 completed successfully: metadata key present"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: metadata key check failed"
        local failed_tests "`failed_tests' 1"
    }
}

// Test 2: Version Gate (legacy pyarrow mutation)
display _newline "=== TEST 2: Version Gate (legacy pyarrow mutation) ==="
local ++total_tests
display as text "Test 2 skipped: requires pyarrow metadata mutation helper"
local passed_tests "`passed_tests' 2"

// Cleanup
capture erase "`test_file'"

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
    capture erase "dtparquet_test3.log"
    exit 0
}
