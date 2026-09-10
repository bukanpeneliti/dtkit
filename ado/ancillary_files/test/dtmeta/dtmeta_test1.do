* dtmeta_test.do
* Comprehensive test suite for dtmeta.ado
* Date: 30may2025

version 16
clear frames
capture log close
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}
log using ado/ancillary_files/test/log/dtmeta_test1.log, replace

// manually drop main program and subroutines
capture program drop dtmeta
capture program drop _makevars
capture program drop _makevarnotes
capture program drop _makevallab
capture program drop _makedtainfo
capture program drop _isempty
capture program drop _labelframes
capture program drop _toexcel
capture program drop _argload
capture program drop _makereport
run ado/dtmeta.ado

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

capture program drop dtrace
program define dtrace
    set trace on        // Enable tracing
    set tracedepth 2    // Set trace depth to 2 levels
    capture noisily `0' // Execute user's command (with error handling)
    set trace off       // Always disable tracing afterward
end

// Display test header
di _n(2) "=========================================="
di "Starting dtmeta Test Suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// Test Case 1: Basic functionality with auto dataset
di _n "=== TEST CASE 1: Auto dataset (basic functionality) ==="
local ++total_tests
sysuse auto, clear
dtmeta
if _rc {
    di as error "Test 1 failed with error " _rc
    local failed_tests "`failed_tests' 1"
    di as text "Error message: " _rc
}
else {
    di as text "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
    
    // Check if frames were created
    frame dir
    foreach fr in _dtvars _dtnotes _dtlabel _dtinfo {
        capture frame `fr': describe
        if _rc {
            di as error "Frame `fr' not created"
        }
        else {
            di as text "Frame `fr' created successfully"
            frame `fr': describe, simple
        }
    }
    
    // Basic content checks
    frame _dtvars: count
    if r(N) == 0 di as error "No variables in _dtvars"
    frame _dtinfo: count
    if r(N) == 0 di as error "No data in _dtinfo"
}

// Test Case 2: nlsw88 dataset (test value labels)
di _n "=== TEST CASE 2: nlsw88 dataset (value labels) ==="
local ++total_tests
sysuse nlsw88, clear
dtmeta
if _rc {
    di as error "Test 2 failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    di as text "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
    
    // Check for value labels
    frame _dtlabel: describe, simple
    frame _dtlabel: count
    if r(N) > 0 {
        di as result "Value labels found: " r(N) " entries"
        frame _dtlabel: list in 1/5, clean noobs
    }
    else {
        di as text "No value labels found"
    }
}

// Test Case 3: Using option
di _n "=== TEST CASE 3: Using option ==="
local ++total_tests
sysuse auto, clear
tempfile autodata
save `autodata'
clear
dtmeta using `autodata'
if _rc {
    di as error "Test 3 failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    di as text "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
    frame dir
}

// Test Case 4: Excel export
di _n "=== TEST CASE 4: Excel export ==="
local ++total_tests
sysuse auto, clear
dtmeta, save("ado/ancillary_files/test/dtmeta/dtmeta_output.xlsx") replace
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    di as text "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
    capture confirm file "test/dtmeta/dtmeta_output.xlsx"
    if _rc {
        di as error "Excel file not created"
    }
    else {
        di as result "Excel file created successfully"
    }
}

// Test Case 5: Dataset with no notes/labels
di _n "=== TEST CASE 5: Empty metadata cases ==="
local ++total_tests
clear
set obs 100
gen x = _n
gen y = x^2
// No labels or notes
dtmeta
if _rc {
    di as error "Test 5 failed with error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    di as text "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
    
    // Check frames for empty content
    foreach fr in _dtvars _dtnotes _dtlabel _dtinfo {
        capture frame `fr': count
        if !_rc {
            di as text "Frame `fr': " r(N) " observations"
        }
    }
}

// Test Case 6: nlswork dataset (large dataset)
di _n "=== TEST CASE 6: nlswork dataset (large dataset) ==="
local ++total_tests
webuse nlswork, clear
dtmeta
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    di as text "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
    
    // Check total observations
    frame _dtvars: count
    di as result "Variables metadata: " r(N) " entries"
    frame _dtinfo: list, clean
    
    // Check for value labels
    frame _dtlabel: count
    if r(N) > 0 {
        di as result "Value labels found: " r(N) " entries"
        frame _dtlabel: list, clean noobs
    }
}

// Test Case 7: Error handling
di _n "=== TEST CASE 7: Error handling ==="
local ++total_tests
local test7_errors 0

// Test with no data in memory
clear
capture dtmeta
if _rc == 0 {
    di as error "Test 7a failed: no data not caught"
    local ++test7_errors
}
else {
    di as result "Test 7a passed: no data handled (error " _rc ")"
}

// Test with Excel output but no save option
sysuse auto, clear
capture dtmeta, excel(sheet("test"))
if _rc == 0 {
    di as error "Test 7b failed: excel without save not caught"
    local ++test7_errors
}
else {
    di as result "Test 7b passed: excel without save handled (error " _rc ")"
}

// Overall Test 7 result
if `test7_errors' > 0 {
    local failed_tests "`failed_tests' 7"
}
else {
    local passed_tests "`passed_tests' 7"
}

// Test Case 8: Excel export with custom filename
di _n "=== TEST CASE 8: Excel export with custom filename ==="
local ++total_tests
sysuse auto, clear
dtmeta, save("ado/ancillary_files/test/dtmeta/no-space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtmeta/no-space.xlsx"
if _rc {
    di as error "Test 8 failed with error " _rc
    local failed_tests "`failed_tests' 8"
}
else {
    di as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}

// Test Case 9: Excel export with custom filename and space
di _n "=== TEST CASE 9: Excel export with custom filename and space ==="
local ++total_tests
sysuse auto, clear
dtmeta, save("ado/ancillary_files/test/dtmeta/file with space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtmeta/file with space.xlsx"

if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}

// Test Case 10: Excel export to non-existent directory
di _n "=== TEST CASE 10: Excel export to non-existent directory ==="
local ++total_tests
sysuse auto, clear
capture dtmeta, save("ado/ancillary_files/test/non-existent/file.xlsx") replace

if _rc == 601 {
    di as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    di as error "Test 10 failed with error " _rc
    local failed_tests "`failed_tests' 10"
}

// Test Case 11: Excel export without extension
di _n "=== TEST CASE 11: Excel export without extension ==="
local ++total_tests
sysuse auto, clear
dtmeta, save("ado/ancillary_files/test/dtmeta/no-extension") replace
capture confirm file "ado/ancillary_files/test/dtmeta/no-extension.xlsx"
if _rc {
    di as error "Test 11 failed with error " _rc
    local failed_tests "`failed_tests' 11"
}
else {
    di as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}

// Test Case 12: Value labels with embedded line breaks (LF/CR)
di _n "=== TEST CASE 12: Line-break value labels (LF/CR) ==="
local ++total_tests
clear
set obs 1
gen byte y_lf = 1
gen byte y_cr = 1
// uselabel round-trips text through a temp do-file, so LF/CR text broke its
// line parser ("in not found", r(111)). Build such labels directly in Mata.
mata: st_vlmodify("dtmeta_lf", 1, "line1" + char(10) + "line2")
label values y_lf dtmeta_lf
mata: st_vlmodify("dtmeta_cr", 1, "carriage" + char(13) + "return")
label values y_cr dtmeta_cr
local t12_err 0
capture dtmeta
if _rc {
    di as error "Test 12 failed: dtmeta returned error " _rc
    local ++t12_err
}
else {
    // Label text must survive byte-identical: LF at 6, length 11;
    // CR at 9, length 15 (carriage + char(13) + return = 8+1+6).
    // Rows sort by vallab, so find each label's row with a per-row
    // vallab comparison first. LF/CR text never enters a macro.
    local t12_lfrow = 0
    local t12_crrow = 0
    frame _dtlabel: local t12_N = _N
    forvalues t12_r = 1/`t12_N' {
        frame _dtlabel: local t12_vl = vallab[`t12_r']
        if "`t12_vl'" == "dtmeta_lf" local t12_lfrow `t12_r'
        if "`t12_vl'" == "dtmeta_cr" local t12_crrow `t12_r'
    }
    if `t12_lfrow' == 0 | `t12_crrow' == 0 {
        di as error "Test 12 failed: label rows not found"
        local ++t12_err
    }
    else {
        frame _dtlabel: mata: st_numscalar("t12_lf_at", strpos(st_sdata(`t12_lfrow', "label"), char(10)))
        frame _dtlabel: mata: st_numscalar("t12_lf_len", strlen(st_sdata(`t12_lfrow', "label")))
        if t12_lf_at != 6 | t12_lf_len != 11 {
            di as error "Test 12 failed: LF text not preserved (at=" t12_lf_at " len=" t12_lf_len ")"
            local ++t12_err
        }
        frame _dtlabel: mata: st_numscalar("t12_cr_at", strpos(st_sdata(`t12_crrow', "label"), char(13)))
        frame _dtlabel: mata: st_numscalar("t12_cr_len", strlen(st_sdata(`t12_crrow', "label")))
        if t12_cr_at != 9 | t12_cr_len != 15 {
            di as error "Test 12 failed: CR text not preserved (at=" t12_cr_at " len=" t12_cr_len ")"
            local ++t12_err
        }
    }
}
if `t12_err' == 0 {
    di as text "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    di as error "Test 12 failed with `t12_err' error(s)"
    local failed_tests "`failed_tests' 12"
}

// Cleanup
frame change default
capture frame drop _dtvars _dtnotes _dtlabel _dtinfo _dtsource

// Test Summary
di _n(2) "=========================================="
di "TEST SUMMARY"
di "=========================================="

// Count passed and failed tests
local num_passed: word count `passed_tests'
local num_failed: word count `failed_tests'

di as text "Total tests run: " as result `total_tests'
di as text "Tests passed: " as result `num_passed' as text " (" as result %4.1f (`num_passed'/`total_tests'*100) as text "%)"
di as text "Tests failed: " as result `num_failed' as text " (" as result %4.1f (`num_failed'/`total_tests'*100) as text "%)"

if `num_passed' > 0 {
    di _n as text "PASSED TESTS:"
    foreach test in `passed_tests' {
        di as result "  PASSED: Test `test'"
    }
}

if `num_failed' > 0 {
    di _n as text "FAILED TESTS:"
    foreach test in `failed_tests' {
        di as error "  FAILED: Test `test'"
    }
}

di _n as text "Overall Status: " _continue
if `num_failed' == 0 {
    di as result "ALL TESTS PASSED!"
}
else {
    di as error "`num_failed' TEST(S) FAILED"
}

// Final summary
di _n(2) "=========================================="
di "Test Suite Completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "Check output above for any errors"
di "=========================================="

log close
