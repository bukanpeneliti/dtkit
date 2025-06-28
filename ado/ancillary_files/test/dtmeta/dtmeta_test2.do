*******************************************************
* dtmeta_test.do
* Comprehensive tests for dtmeta.ado
* Place this file in test/dtmeta/dtmeta_test.do
*******************************************************

version 16
clear frames
capture log close
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}
log using test/dtmeta/dtmeta_test2.log, replace

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

*******************************************************
* Test 1: dtmeta with no data loaded — should error
*******************************************************
display as text "Test 1: dtmeta without data"
local ++total_tests
dtmeta
if _rc {
    display as result "  PASS: error as expected (rc=`_rc')"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "  FAIL: no error when no data loaded"
    local failed_tests "`failed_tests' 1"
}

*******************************************************
* Test 2: dtmeta on in‐memory auto.dta
*******************************************************
display as text "Test 2: dtmeta on in‐memory auto"
local ++total_tests
sysuse auto, clear
dtmeta
if _rc {
    display as error "  FAIL: dtmeta on auto returned error (rc=`_rc')"
    local failed_tests "`failed_tests' 2"
}
else {
    display as result "  PASS: dtmeta on auto ran OK"
    local passed_tests "`passed_tests' 2"
    foreach fr in _dtvars _dtnotes _dtlabel _dtinfo {
        capture confirm frame `fr'
        if _rc {
            display as error "    FAIL: frame `fr' not created"
        }
        else {
            display as result "    PASS: frame `fr' exists"
        }
    }
}

*******************************************************
* Test 3: dtmeta using auto.dta (disk file)
*******************************************************
display as text "Test 3: dtmeta using auto.dta"
local ++total_tests
dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta"
if _rc {
    display as error "  FAIL: dtmeta using auto.dta error (rc=`_rc')"
    local failed_tests "`failed_tests' 3"
}
else {
    display as result "  PASS: dtmeta using auto.dta ran OK"
    local passed_tests "`passed_tests' 3"
}

*******************************************************
* Test 4: excel export (no replace)
*******************************************************
display as text "Test 4: dtmeta using auto, excel export"
local ++total_tests
local xfile = "dtmeta_auto.xlsx"
dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", save("`xfile'")
if _rc {
    display as error "  FAIL: excel export error (rc=`_rc')"
    local failed_tests "`failed_tests' 4"
}
else {
    display as result "  PASS: excel export ran OK"
    local passed_tests "`passed_tests' 4"
    capture confirm file "`xfile'"
    if !_rc {
        display as result "    PASS: file `xfile' created"
    }
    else {
        display as error "    FAIL: file `xfile' not found"
    }
    capture erase "`xfile'"
}

*******************************************************
* Test 5: excel export with replace
*******************************************************
display as text "Test 5: dtmeta using auto, excel replace"
local ++total_tests
local xfile2 = "dtmeta_auto2.xlsx"
* first create
dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", save("`xfile2'")
* now run with replace option
// set trace on
// set tracedepth 2
dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", save("`xfile2'") replace
// set trace off
if _rc {
    display as error "  FAIL: excel replace error (rc=`_rc')"
    local failed_tests "`failed_tests' 5"
}
else {
    display as result "  PASS: excel replace ran OK"
    local passed_tests "`passed_tests' 5"
}
capture erase "`xfile2'"

*******************************************************
* Test 6: clear without using — should error
*******************************************************
display as text "Test 6a: dtmeta , clear"
local ++total_tests
local test6_errors 0
dtmeta, clear
if _rc {
    display as result "  PASS: clear without using errored (rc=`_rc')"
}
else {
    display as error "  FAIL: clear without using should have errored"
    local ++test6_errors
}

display as text "Test 6b: dtmeta using ..., replace (without clear)"
dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", replace
if _rc {
    display as result "  PASS: replace without clear errored (rc=`_rc')"
}
else {
    display as error "  FAIL: replace without clear should have errored"
    local ++test6_errors
}

if `test6_errors' > 0 {
    local failed_tests "`failed_tests' 6"
}
else {
    local passed_tests "`passed_tests' 6"
}

*******************************************************
* Test 7: dtmeta using remote nlsw88.dta, clear
*******************************************************
display as text "Test 7: dtmeta using remote nlsw88.dta, clear"
local ++total_tests
dtmeta using "https://www.stata-press.com/data/r18/nlsw88.dta", clear
if _rc {
    display as error "  FAIL: remote dtmeta error (rc=`_rc')"
    local failed_tests "`failed_tests' 7"
}
else {
    display as result "  PASS: remote dtmeta ran OK"
    local passed_tests "`passed_tests' 7"
}

*******************************************************
* Test 8: dtmeta using remote nlswork.dta, clear
*******************************************************
display as text "Test 8: dtmeta using remote nlswork.dta, clear"
local ++total_tests
dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta", clear
if _rc {
    display as error "  FAIL: remote dtmeta error (rc=`_rc')"
    local failed_tests "`failed_tests' 8"
}
else {
    display as result "  PASS: remote dtmeta ran OK"
    local passed_tests "`passed_tests' 8"
}

*******************************************************
* Test 9: no variable notes frame dropped
*******************************************************
display as text "Test 9: frame _dtnotes dropped when no notes"
local ++total_tests
sysuse auto, clear
dtmeta
capture confirm frame _dtnotes
if _rc {
    display as result "  PASS: _dtnotes not present for auto"
    local passed_tests "`passed_tests' 9"
}
else {
    display as error "  FAIL: _dtnotes present when it should be dropped"
    local failed_tests "`failed_tests' 9"
}

*******************************************************
* Test 10: rclass returns source_frame
*******************************************************
display as text "Test 10: r(source_frame) macro"
local ++total_tests
sysuse auto, clear
dtmeta
if "`r(source_frame)'" != "" {
    display as result "  PASS: r(source_frame) = `r(source_frame)'"
    local passed_tests "`passed_tests' 10"
}
else {
    display as error "  FAIL: r(source_frame) is empty"
    local failed_tests "`failed_tests' 10"
}

*******************************************************
* Test 11: show report
*******************************************************
display as text "Test 11: reporting results"
local ++total_tests
sysuse auto, clear
dtmeta
pwf
dtmeta, report
if _rc {
    display as error "  FAIL: report functionality error (rc=`_rc')"
    local failed_tests "`failed_tests' 11"
}
else {
    display as result "  PASS: report functionality ran OK"
    local passed_tests "`passed_tests' 11"
}

*******************************************************
* Finalize
*******************************************************

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
        di as result "  ✓ Test `test'"
    }
}

if `num_failed' > 0 {
    di _n as text "FAILED TESTS:"
    foreach test in `failed_tests' {
        di as error "  ✗ Test `test'"
    }
}

di _n as text "Overall Status: " _continue
if `num_failed' == 0 {
    di as result "ALL TESTS PASSED! ✓"
}
else {
    di as error "`num_failed' TEST(S) FAILED ✗"
}

log close
display as text "All tests completed — see dtmeta_test.log for details."
