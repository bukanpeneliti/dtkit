* dtparquet_test1.do
* Comprehensive test suite for dtparquet Phase 1
* Date: Jan 12, 2026

version 16
clear frames
capture log close
cd d:/OneDrive/MyWork/00personal/stata/dtkit

log using ado/ancillary_files/test/log/dtparquet_test1.log, replace

// Load programs from ado directory
discard
local ado_plus = c(sysdir_plus)
copy ado/dtparquet.ado "`ado_plus'd/dtparquet.ado", replace
copy ado/dtparquet.py "`ado_plus'd/dtparquet.py", replace

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

 // Display test header
    display _newline(2) "=========================================="
    display "Starting dtparquet Phase 1 Test Suite"
    display "Timestamp: " c(current_date) " " c(current_time)
    display "==========================================" _newline

// Test Case 1: _check_python - Python not found - this will always fail since python is installed alreade
display _newline "=== TEST CASE 1: _check_python - Python Not Found ==="
local ++total_tests
clear
capture set python_exec ""
capture dtparquet_save "test_case1.parquet", replace
if _rc == 198 {
    display as result "Test 1 completed successfully (caught python not found error)"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: expected error 198 (python not found), got " _rc
    local failed_tests "`failed_tests' 1"
}

// Test Case 2: Basic Save and Use roundtrip with all types
display _newline "=== TEST CASE 2: Basic Save and Use (All Data Types) ==="
local ++total_tests
python query
if r(initialized) == 1 {
    display as text "Python already initialized. Using current Python installation."
}
else {
    set python_exec "C:/Users/hafiz/AppData/Local/Python/pythoncore-3.14-64/python.exe"
}
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

dtparquet save "test_case2.parquet", replace
if _rc {
    display as error "Test 2 Save failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    clear
    dtparquet use using "test_case2.parquet"
    if _rc {
        display as error "Test 2 Use failed with error " _rc
        local failed_tests "`failed_tests' 2"
    }
    else {
        // Verification
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

// Test Case 3: Metadata Preservation
display _newline "=== TEST CASE 3: Metadata Preservation (Labels and Notes) ==="
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

dtparquet save "test_case3.parquet", replace
clear
dtparquet use using "test_case3.parquet"

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
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 metadata verification failed"
    local failed_tests "`failed_tests' 3"
}
else {
    display as error "Test 2 metadata verification failed"
    local failed_tests "`failed_tests' 2"
}

// Test Case 4: Varlist Subsetting
display _newline "=== TEST CASE 4: Varlist Subsetting ==="
local ++total_tests
clear
set obs 1
generate a = 1
generate b = 2
generate c = 3
dtparquet save "test_case4.parquet", replace

clear
dtparquet use a c using "test_case4.parquet"
if _rc == 0 & c(k) == 2 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 failed: expected 2 variables, got " c(k)
    local failed_tests "`failed_tests' 4"
}
else {
    display as error "Test 3 failed: expected 2 variables, got " c(k)
    local failed_tests "`failed_tests' 3"
}

// Test Case 5: nolabel option
display _newline "=== TEST CASE 5: nolabel Option ==="
local ++total_tests
clear
set obs 1
generate x = 1
label variable x "Label"
dtparquet save "test_case5.parquet", replace

clear
dtparquet use using "test_case5.parquet", nolabel
local vlab : var label x
if "`vlab'" == "" {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed: label should have been empty"
    local failed_tests "`failed_tests' 5"
}

// Test Case 6: IF/IN conditions
display _newline "=== TEST CASE 6: IF/IN conditions ==="
local ++total_tests
clear
set obs 10
generate id = _n
dtparquet save "test_case6.parquet", replace

clear
dtparquet use using "test_case6.parquet" if id > 5 in 1/8
if _rc == 0 & c(N) == 3 {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: expected 3 observations, got " c(N)
    local failed_tests "`failed_tests' 6"
}

// Test Case 7: Error Handling (Missing file)
display _newline "=== TEST CASE 7: Error Handling (Missing File) ==="
local ++total_tests
clear
capture dtparquet use using "non_existent.parquet"
if _rc != 0 {
    display as result "Test 7 completed successfully (caught missing file)"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: did not catch missing file"
    local failed_tests "`failed_tests' 7"
}

// Cleanup
capture erase "test_case1.parquet"
capture erase "test_case2.parquet"
capture erase "test_case3.parquet"
capture erase "test_case4.parquet"
capture erase "test_case5.parquet"
capture erase "test_case6.parquet"
capture erase "test.parquet"
capture erase "test_orig.dta"
capture set python_exec ""

// Test Summary
display _newline(2) "=========================================="
display "TEST SUMMARY"
display "=========================================="

local num_passed: word count `passed_tests'
local num_failed: word count `failed_tests'

display as text "Total tests run: " as result `total_tests'
display as text "Tests passed: " as result `num_passed' as text " (" as result %4.1f (`num_passed'/`total_tests'*100) as text "%)"
display as text "Tests failed: " as result `num_failed' as text " (" as result %4.1f (`num_failed'/`total_tests'*100) as text "%)"

if `num_passed' > 0 {
    display _newline as text "PASSED TESTS:"
    foreach test in `passed_tests' {
        display as result "  Test `test'"
    }
}

if `num_failed' > 0 {
    display _newline as text "FAILED TESTS:"
    foreach test in `failed_tests' {
        display as error "  Test `test'"
    }
}

display _newline as text "Overall Status: " _continue
if `num_failed' == 0 {
    display as result "ALL TESTS PASSED!"
}
else {
    display as error "`num_failed' TEST(S) FAILED"
}

display _newline(2) "=========================================="
display "dtparquet Test Suite Completed"
display "Timestamp: " c(current_date) " " c(current_time)
display "=========================================="

log close
