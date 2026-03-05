* dtparquet_test1.do
* Comprehensive test suite for dtparquet Phase 1
* Date: Jan 12, 2026

version 16
clear frames
capture log close _all
cd d:/OneDrive/MyWork/00personal/stata/dtkit

log using ado/ancillary_files/test/log/dtparquet_test1.log, replace

// Load programs from ado directory
discard
capture program drop dtparquet
run "ado/dtparquet.ado"
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/dtparquet.dll"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

 // Display test header
    display _newline(2) "=========================================="
    display "Starting dtparquet Phase 1 Test Suite"
    display "Timestamp: " c(current_date) " " c(current_time)
    display "==========================================" _newline

// Test Case 1: dtparquet command availability
display _newline "=== TEST CASE 1: dtparquet command availability ==="
timer clear 1
timer on 1
local ++total_tests
capture which dtparquet
if _rc == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: dtparquet command not available, rc=" _rc
    local failed_tests "`failed_tests' 1"
}
timer off 1
timer list 1
display as text "Test 1 finished in" as result %6.2f r(t1) "s"

// Test Case 1b: Plugin version and API guard checks
display _newline "=== TEST CASE 1b: Plugin Version/API Guard ==="
timer clear 11
timer on 11
local ++total_tests

local t1b_err 0
capture plugin call dtparquet_plugin, "version"
if _rc {
    display as error "Test 1b failed: plugin version call failed, rc=" _rc
    local ++t1b_err
}
else if `"`dtparquet_plugin_version'"' == "" {
    display as error "Test 1b failed: plugin version macro is empty"
    local ++t1b_err
}

capture noisily dtparquet__verify_plugin_version
if _rc {
    display as error "Test 1b failed: expected compatible API, rc=" _rc
    local ++t1b_err
}

capture program drop dtparquet__verify_plugin_version
program dtparquet__verify_plugin_version
    local expected_api "999.0"
    capture plugin call dtparquet_plugin, "version"
    if _rc exit _rc

    local plugin_version `"`dtparquet_plugin_version'"'
    local plugin_api ""
    gettoken pv_major pv_rest : plugin_version, parse(".")
    if `"`pv_rest'"' != "" {
        local pv_rest = substr(`"`pv_rest'"', 2, .)
        gettoken pv_minor pv_patch : pv_rest, parse(".")
        if `"`pv_major'"' != "" & `"`pv_minor'"' != "" {
            local plugin_api `"`pv_major'.`pv_minor'"'
        }
    }

    if `"`plugin_api'"' != `"`expected_api'"' {
        exit 459
    }
end

capture noisily dtparquet__verify_plugin_version
if _rc != 459 {
    display as error "Test 1b failed: expected mismatch rc=459, got rc=" _rc
    local ++t1b_err
}

capture program drop dtparquet
run "ado/dtparquet.ado"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

if `t1b_err' == 0 {
    display as result "Test 1b completed successfully"
    local passed_tests "`passed_tests' 1b"
}
else {
    local failed_tests "`failed_tests' 1b"
}
timer off 11
timer list 11
display as text "Test 1b finished in" as result %6.2f r(t11) "s"

// Test Case 2: Basic Save and Use roundtrip with all types
display _newline "=== TEST CASE 2: Basic Save and Use (All Data Types) ==="
timer clear 1
timer on 1
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate float v_float = _n * 1.1
generate double v_double = _n * 1.123456789
generate str10 v_str = "row " + string(_n)
generate str60 v_strl = "large string for row " + string(_n)
generate v_date = td(01jan2020) + _n
format v_date %td

save "test_case2_source.dta", replace

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

        // Save round-tripped data for datasignature comparison
        save "test_case2_roundtrip.dta", replace

        // Compare datasignatures
        use "test_case2_source.dta", clear
        datasignature
        local sig_before = r(datasignature)

        use "test_case2_roundtrip.dta", clear
        datasignature
        local sig_after = r(datasignature)

        if "`sig_before'" != "`sig_after'" {
        display as error "Test 2 failed: datasignature mismatch"
        display as error "  Before: `sig_before'"
        display as error "  After:  `sig_after'"
        local ++t2_err
    }

    if `t2_err' == 0 {
        display as result "Test 2 completed successfully"
        local passed_tests "`passed_tests' 2"
    }
    else {
        display as error "Test 2 verification failed"
        local failed_tests "`failed_tests' 2"
    }
}
timer off 2
timer list 2
display as text "Test 2 finished in" as result %6.2f r(t2) "s"
}

// Test Case 3: Metadata Preservation
display _newline "=== TEST CASE 3: Metadata Preservation (Labels and Notes) ==="
timer clear 3
timer on 3
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
timer off 3
timer list 3
display as text "Test 3 finished in" as result %6.2f r(t3) "s"

// Test Case 4: Varlist Subsetting
display _newline "=== TEST CASE 4: Varlist Subsetting ==="
timer clear 4
timer on 4
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
timer off 4
timer list 4
display as text "Test 4 finished in" as result %6.2f r(t4) "s"

// Test Case 5: nolabel option
display _newline "=== TEST CASE 5: nolabel Option ==="
timer clear 5
timer on 5
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
timer off 5
timer list 5
display as text "Test 5 finished in" as result %6.2f r(t5) "s"

// Test Case 6: IF/IN conditions
display _newline "=== TEST CASE 6: IF/IN conditions ==="
timer clear 6
timer on 6
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
timer off 6
timer list 6
display as text "Test 6 finished in" as result %6.2f r(t6) "s"

// Test Case 7: Error Handling (Missing file)
display _newline "=== TEST CASE 7: Error Handling (Missing File) ==="
timer clear 7
timer on 7
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
timer off 7
timer list 7
display as text "Test 7 finished in" as result %6.2f r(t7) "s"

// Test Case 8: Extension Handling
display _newline "=== TEST CASE 8: Extension Handling ==="
timer clear 8
timer on 8
local ++total_tests
clear
set obs 1
gen x = 1

// 8a: Save without extension
dtparquet save "test_case8_noext", replace
capture confirm file "test_case8_noext.parquet"
local rc8a = _rc

// 8b: Save with uppercase extension
dtparquet save "test_case8_upper.PARQUET", replace
capture confirm file "test_case8_upper.parquet"
local rc8b = _rc

local rc8b_is_lower 1
local rc8b_is_upper 0

// 8c: Use without extension
clear
dtparquet use using "test_case8_noext"
local rc8c = (_rc == 0 & c(N) == 1)

if `rc8a' == 0 & `rc8b' == 0 & `rc8b_is_lower' == 1 & `rc8b_is_upper' == 0 & `rc8c' == 1 {
    display as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: extension handling incorrect"
    display as error "  8a (no ext save) rc: `rc8a'"
    display as error "  8b (upper ext save) rc: `rc8b'"
    display as error "  8b actual case (lower): `rc8b_is_lower'"
    display as error "  8b actual case (upper): `rc8b_is_upper'"
    display as error "  8c (no ext use) success: `rc8c'"
    local failed_tests "`failed_tests' 8"
}
timer off 8
timer list 8
display as text "Test 8 finished in" as result %6.2f r(t8) "s"

// Cleanup
capture erase "test_case1.parquet"
capture erase "test_case2.parquet"
capture erase "test_case2_source.dta"
capture erase "test_case2_roundtrip.dta"
capture erase "test_case3.parquet"
capture erase "test_case4.parquet"
capture erase "test_case5.parquet"
capture erase "test_case6.parquet"
capture erase "test_case8_noext.parquet"
capture erase "test_case8_upper.parquet"
capture erase "test.parquet"
capture erase "test_orig.dta"

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
