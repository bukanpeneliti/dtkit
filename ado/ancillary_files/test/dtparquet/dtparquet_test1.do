* dtparquet_test1.do
* Comprehensive test suite for dtparquet Phase 1
* Date: Jan 12, 2026

version 16
clear frames
capture log close

// Environment setup
// Assume we are in the project root
adopath ++ "`c(pwd)'/ado"

// Ensure log directory exists
capture mkdir "ado/ancillary_files/test/log"

log using ado/ancillary_files/test/log/dtparquet_test1.log, replace

// Setup Python environment FIRST
python:
import sys
import os
sys.path.insert(0, os.path.join(os.getcwd(), "ado"))
end

// Manually drop and load programs
capture program drop dtparquet
capture program drop dtparquet_save
capture program drop dtparquet_use
capture program drop _apply_dtmeta
run ado/dtparquet.ado

python:
import dtparquet
print(f"dtparquet.py path: {dtparquet.__file__}")
end

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
    di _newline(2) "=========================================="
    di "Starting dtparquet Phase 1 Test Suite"
    di "Timestamp: " c(current_date) " " c(current_time)
    di "==========================================" _newline

// Test Case 1: Basic Save and Use roundtrip with all types
di _newline "=== TEST CASE 1: Basic Save and Use (All Data Types) ==="
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate float v_float = _n * 1.1
generate double v_double = _n * 1.123456789
generate str10 v_str = "row " + string(_n)
generate strL v_strl = "large string for row " + string(_n)
generate v_date = td(01jan2020) + _n
format v_date %td

dtparquet save "test_case1.parquet", replace
if _rc {
    di as error "Test 1 Save failed with error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    clear
    dtparquet use using "test_case1.parquet"
    if _rc {
        di as error "Test 1 Use failed with error " _rc
        local failed_tests "`failed_tests' 1"
    }
    else {
        // Verification
        local t1_err 0
        capture assert v_byte == _n
        if _rc local ++t1_err
        capture assert v_str == "row " + string(_n)
        if _rc local ++t1_err
        capture assert v_date == td(01jan2020) + _n
        if _rc local ++t1_err
        
        if `t1_err' == 0 {
            di as result "Test 1 completed successfully"
            local passed_tests "`passed_tests' 1"
        }
        else {
            di as error "Test 1 verification failed"
            local failed_tests "`failed_tests' 1"
        }
    }
}

// Test Case 2: Metadata Preservation
di _newline "=== TEST CASE 2: Metadata Preservation (Labels and Notes) ==="
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

dtparquet save "test_case2.parquet", replace
clear
dtparquet use using "test_case2.parquet"

local t2_err 0
local dlab : data label
if "`dlab'" != "My Test Dataset" local ++t2_err

local vlab : var label x
if "`vlab'" != "Variable X Label" local ++t2_err

local vlbl : value label x
if "`vlbl'" != "xlbl" local ++t2_err

// Check value label content
local lbl1 : label xlbl 1
if "`lbl1'" != "One" local ++t2_err

if `t2_err' == 0 {
    di as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    di as error "Test 2 metadata verification failed"
    local failed_tests "`failed_tests' 2"
}

// Test Case 3: Varlist Subsetting
di _newline "=== TEST CASE 3: Varlist Subsetting ==="
local ++total_tests
clear
set obs 1
generate a = 1
generate b = 2
generate c = 3
dtparquet save "test_case3.parquet", replace

clear
dtparquet use a c using "test_case3.parquet"
if _rc == 0 & c(k) == 2 {
    di as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    di as error "Test 3 failed: expected 2 variables, got " c(k)
    local failed_tests "`failed_tests' 3"
}

// Test Case 4: nolabel option
di _newline "=== TEST CASE 4: nolabel Option ==="
local ++total_tests
clear
set obs 1
generate x = 1
label variable x "Label"
dtparquet save "test_case4.parquet", replace

clear
dtparquet use using "test_case4.parquet", nolabel
local vlab : var label x
if "`vlab'" == "" {
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    di as error "Test 4 failed: label should have been empty"
    local failed_tests "`failed_tests' 4"
}

// Test Case 5: IF/IN conditions
di _newline "=== TEST CASE 5: IF/IN conditions ==="
local ++total_tests
clear
set obs 10
generate id = _n
dtparquet save "test_case5.parquet", replace

clear
dtparquet use using "test_case5.parquet" if id > 5 in 1/8
if _rc == 0 & c(N) == 3 {
    di as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    di as error "Test 5 failed: expected 3 observations, got " c(N)
    local failed_tests "`failed_tests' 5"
}

// Test Case 6: Error Handling (Missing file)
di _newline "=== TEST CASE 6: Error Handling (Missing File) ==="
local ++total_tests
clear
capture dtparquet use using "non_existent.parquet"
if _rc != 0 {
    di as result "Test 6 completed successfully (caught missing file)"
    local passed_tests "`passed_tests' 6"
}
else {
    di as error "Test 6 failed: did not catch missing file"
    local failed_tests "`failed_tests' 6"
}

// Cleanup
capture erase "test_case1.parquet"
capture erase "test_case2.parquet"
capture erase "test_case3.parquet"
capture erase "test_case4.parquet"
capture erase "test_case5.parquet"
capture erase "test.parquet"
capture erase "test_orig.dta"

// Test Summary
di _newline(2) "=========================================="
di "TEST SUMMARY"
di "=========================================="

local num_passed: word count `passed_tests'
local num_failed: word count `failed_tests'

di as text "Total tests run: " as result `total_tests'
di as text "Tests passed: " as result `num_passed' as text " (" as result %4.1f (`num_passed'/`total_tests'*100) as text "%)"
di as text "Tests failed: " as result `num_failed' as text " (" as result %4.1f (`num_failed'/`total_tests'*100) as text "%)"

if `num_passed' > 0 {
    di _newline as text "PASSED TESTS:"
    foreach test in `passed_tests' {
        di as result "  Test `test'"
    }
}

if `num_failed' > 0 {
    di _newline as text "FAILED TESTS:"
    foreach test in `failed_tests' {
        di as error "  Test `test'"
    }
}

di _newline as text "Overall Status: " _continue
if `num_failed' == 0 {
    di as result "ALL TESTS PASSED!"
}
else {
    di as error "`num_failed' TEST(S) FAILED"
}

di _newline(2) "=========================================="
di "dtparquet Test Suite Completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "=========================================="

log close
