* dtparquet_test2.do
* Comprehensive test suite for dtparquet Phase 2 (export/import with atomic writes)
* Date: Jan 13, 2026

version 16
clear frames
capture log close
cd d:/OneDrive/MyWork/00personal/stata/dtkit

log using ado/ancillary_files/test/log/dtparquet_test2.log, replace

// Load programs from ado directory
discard
adopath ++ "D:/OneDrive/MyWork/00personal/stata/dtkit/ado"
python: import sys; sys.path.insert(0, "D:/OneDrive/MyWork/00personal/stata/dtkit/ado")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Phase 2 Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Ensure Python is configured
python query
if r(initialized) != 1 {
    set python_exec "C:/Users/hafiz/AppData/Local/Python/pythoncore-3.14-64/python.exe"
}

// Test Case 1: Basic export (.dta -> Parquet)
display _newline "=== TEST CASE 1: Basic Export ==="
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate float v_float = _n * 1.1
generate double v_double = _n * 1.123456789
generate str10 v_str = "row " + string(_n)
generate v_date = td(01jan2020) + _n
format v_date %td

// Save test dataset
save "test_export_source.dta", replace

// Export to parquet
dtparquet export "test_export_target.parquet" using "test_export_source.dta", replace
if _rc {
    display as error "Test 1 Export failed with error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    // Verify parquet file exists
    capture confirm file "test_export_target.parquet"
    if _rc {
        display as error "Test 1 failed: parquet file not created"
        local failed_tests "`failed_tests' 1"
    }
    else {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
}

// Test Case 2: Basic import (Parquet -> .dta)
display _newline "=== TEST CASE 2: Basic Import ==="
local ++total_tests
// Use the parquet file from test 1
dtparquet import "test_import_target.dta" using "test_export_target.parquet", replace
if _rc {
    display as error "Test 2 Import failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    // Verify .dta file exists
    capture confirm file "test_import_target.dta"
    if _rc {
        display as error "Test 2 failed: .dta file not created"
        local failed_tests "`failed_tests' 2"
    }
    else {
        // Load and verify data
        use "test_import_target.dta", clear
        local t2_err 0
        capture assert v_byte == _n
        if _rc local ++t2_err
        capture assert v_str == "row " + string(_n)
        if _rc local ++t2_err
        capture assert v_date == td(01jan2020) + _n
        if _rc local ++t2_err
        
        if `t2_err' == 0 {
            display as result "Test 2 completed successfully"
            local passed_tests "`passed_tests' 2"
        }
        else {
            display as error "Test 2 verification failed"
            local failed_tests "`failed_tests' 2"
        }
    }
}

// Test Case 3: Same datasignature round-trip
display _newline "=== TEST CASE 3: Metadata Preservation ==="
local ++total_tests
clear
use "test_export_source.dta", clear
datasignature
local sig_before = r(datasignature)
use "test_import_target.dta", clear
datasignature
local sig_after = r(datasignature)
if "`sig_before'" == "`sig_after'" {
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: datasignature mismatch"
    local failed_tests "`failed_tests' 3"
}

// Test Case 4: Metadata preservation in export/import
display _newline "=== TEST CASE 4: Metadata Preservation ==="
local ++total_tests
clear
set obs 5
generate x = _n
label variable x "Variable X Label"
label define xlbl 1 "One" 2 "Two"
label values x xlbl
notes x: "This is a note for X"
notes: "Dataset wide note"
label data "My Test Dataset"

save "test_meta_source.dta", replace
dtparquet export "test_meta_target.parquet" using "test_meta_source.dta", replace

dtparquet import "test_meta_import.dta" using "test_meta_target.parquet", replace

// Load imported .dta and verify metadata
use "test_meta_import.dta", clear

local t3_err 0
local dlab : data label
if "`dlab'" != "My Test Dataset" local ++t3_err

local vlab : var label x
if "`vlab'" != "Variable X Label" local ++t3_err

local vlbl : value label x
if "`vlbl'" != "xlbl" local ++t3_err

// Check value label content
local lbl1 : label xlbl 1
if "`lbl1'" != "One" local ++t3_err

if `t3_err' == 0 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 metadata verification failed"
    local failed_tests "`failed_tests' 4"
}

// Test Case 5: nolabel option in export/import
display _newline "=== TEST CASE 5: nolabel Option ==="
local ++total_tests
clear
set obs 1
generate x = 1
label variable x "Label"
save "test_nolabel_source.dta", replace

// Drop any existing metadata frames
foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
    capture frame drop `fr'
}

dtparquet export "test_nolabel_target.parquet" using "test_nolabel_source.dta", replace nolabel

// COMPLETELY fresh Stata session simulation
discard
clear
set obs 0

// Manually erase if exists to be sure
capture erase "test_nolabel_import.dta"

dtparquet import "test_nolabel_import.dta" using "test_nolabel_target.parquet", replace nolabel

// Check file on disk directly using a fresh use
use "test_nolabel_import.dta", clear
local vlab : var label x
display "DEBUG: Final label for x is '`vlab''"
if "`vlab'" == "" {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed: label should have been empty, but was '`vlab''"
    local failed_tests "`failed_tests' 5"
}

// Test Case 6: Atomic write safety (replace protection)
display _newline "=== TEST CASE 6: Replace Protection ==="
local ++total_tests
clear
set obs 1
generate x = 1
save "test_replace_source.dta", replace

// Create target file
dtparquet export "test_replace_target.parquet" using "test_replace_source.dta", replace

// Try to export again without replace - should fail
capture dtparquet export "test_replace_target.parquet" using "test_replace_source.dta"
if _rc == 602 {
    display as result "Test 6 completed successfully (caught replace error)"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: expected error 602 (file already exists), got " _rc
    local failed_tests "`failed_tests' 6"
}

// Test Case 7: Frame isolation (current dataset not affected)
display _newline "=== TEST CASE 7: Frame Isolation ==="
local ++total_tests
clear
set obs 5
generate original = _n
local orig_vars = c(k)
local orig_obs = c(N)

// Export should not affect current dataset
dtparquet export "test_isolation.parquet" using "test_replace_source.dta", replace
if _rc == 0 & c(k) == `orig_vars' & c(N) == `orig_obs' {
    display as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: current dataset was modified"
    local failed_tests "`failed_tests' 7"
}

// Test Case 8: Cleanup of orphaned .tmp files
display _newline "=== TEST CASE 8: Orphaned .tmp File Cleanup ==="
local ++total_tests
// Create a dummy .tmp file
copy "test_export_target.parquet" "test_cleanup.parquet.tmp", replace

// Run dtparquet command which triggers cleanup
clear
set obs 1
generate x = 1
dtparquet save "test_cleanup_dummy.parquet", replace

// Check if .tmp file was cleaned up
capture confirm file "test_cleanup.parquet.tmp"
if _rc {
    display as result "Test 8 completed successfully (.tmp file cleaned up)"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: .tmp file still exists"
    local failed_tests "`failed_tests' 8"
}

// Test Case 9: Error handling (non-existent source file)
display _newline "=== TEST CASE 9: Error Handling ==="
local ++total_tests
capture dtparquet export "test_error.parquet" using "non_existent.dta", replace
if _rc == 601 {
    display as result "Test 9 completed successfully (caught missing file error)"
    local passed_tests "`passed_tests' 9"
}
else {
    display as error "Test 9 failed: expected error 601 (file not found), got " _rc
    local failed_tests "`failed_tests' 9"
}

// Cleanup test files
display _newline "=== Cleaning up test files ==="
local testfiles "test_export_source.dta test_export_target.parquet test_import_target.dta"
local testfiles "`testfiles' test_meta_source.dta test_meta_target.parquet test_meta_import.dta"
local testfiles "`testfiles' test_nolabel_source.dta test_nolabel_target.parquet test_nolabel_import.dta"
local testfiles "`testfiles' test_replace_source.dta test_replace_target.parquet test_isolation.parquet"
local testfiles "`testfiles' test_cleanup_dummy.parquet"

foreach file of local testfiles {
    // capture erase "`file'"
}

// Summary
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