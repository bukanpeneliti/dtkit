*******************************************************
* dtfreq_test.do
* Comprehensive testing of dtfreq.ado
* Author: Hafiz
* Date: 01Jun2025
* Location: test/dtfreq/
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
log using ado/ancillary_files/test/log/dtfreq_test2.log, replace

// manually drop main program and subroutines
capture program drop dtfreq
capture program drop _xtab
capture program drop _xtab_core
capture program drop _binreshape
capture program drop _crosstotal
capture program drop _labelvars
capture program drop _toexcel
capture program drop _formatvars
capture program drop _argcheck
capture program drop _argload
capture mata: mata drop _xtab_core_calc()
run ado/dtfreq.ado

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

* 2.1 Basic one-way frequency on a numeric variable (mpg)
local ++total_tests
sysuse auto, clear
dtrace dtfreq mpg
if _rc {
    di as error "[Test 1] FAIL: dtfreq mpg failed."
    local failed_tests "`failed_tests' 1"
}
else {
    di as result "[Test 1] PASS: One-way frequency on mpg"
    local passed_tests "`passed_tests' 1"
}

* 2.2 One-way frequency with "binary" on a binary variable (foreign)
local ++total_tests
sysuse auto, clear
dtrace dtfreq foreign, binary
if _rc {
    di as error "[Test 2] FAIL: dtfreq foreign, binary failed."
    local failed_tests "`failed_tests' 2"
}
else {
    di as result "[Test 2] PASS: One-way binary frequency on foreign"
    local passed_tests "`passed_tests' 2"
}

* 2.3 One-way frequency with if/in and weights
local ++total_tests
sysuse nlsw88, clear
dtrace dtfreq wage [aweight=south] if wage < 5 & wage > 0, type(prop)
if _rc {
    di as error "[Test 3] FAIL: dtfreq wage with if/weight failed."
    local failed_tests "`failed_tests' 3"
}
else {
    di as result "[Test 3] PASS: One-way frequency on wage with if and weights"
    local passed_tests "`passed_tests' 3"
}

*------------------------------------------------------------------------
* 3.  Two-way (cross-tab) tests
*------------------------------------------------------------------------

* 3.1 Two-way frequency: rep78 x foreign
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign)
if _rc {
    di as error "[Test 4] FAIL: dtfreq rep78, cross(foreign) failed."
    local failed_tests "`failed_tests' 4"
}
else {
    di as result "[Test 4] PASS: Two-way frequency rep78 x foreign"
    local passed_tests "`passed_tests' 4"
}

* 3.2 Two-way with by(): rep78 by foreign (one-way per foreign level + total)
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, by(foreign)
if _rc {
    di as error "[Test 5] FAIL: dtfreq rep78, by(foreign) failed."
    local failed_tests "`failed_tests' 5"
}
else {
    di as result "[Test 5] PASS: One-way frequency of rep78 by foreign levels + total"
    local passed_tests "`passed_tests' 5"
}

* 3.3 Full by() + cross(): rep78 by foreign cross mpg
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, by(foreign) cross(mpg)
if _rc {
    di as error "[Test 6] FAIL (or WARNING): rep78 by(foreign) cross(mpg) produced error."
    local failed_tests "`failed_tests' 6"
}
else {
    di as result "[Test 6] PASS (or WARNING): rep78 by(foreign) cross(mpg)"
    local passed_tests "`passed_tests' 6"
}

* 3.4 Two-way with STATs and TYpe: rep78 x foreign, request row & col stats and prop & pct
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) stats(row col) type(prop pct)
if _rc {
    di as error "[Test 7] FAIL: dtfreq rep78, cross(foreign) STATs(row col) TYpe(prop pct)"
    local failed_tests "`failed_tests' 7"
}
else {
    di as result "[Test 7] PASS: Two-way with STATs(row col) and TYpe(prop pct)"
    local passed_tests "`passed_tests' 7"
}

* 3.5 Error condition: cross() without numeric var
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(make)
if _rc == 109 {
    di as result "[Test 8] PASS: Proper error when cross(make) (non-numeric)."
    local passed_tests "`passed_tests' 8"
}
else if _rc {
    di as error "[Test 8] FAIL: dtfreq rep78, cross(make) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 8"
}
else {
    di as error "[Test 8] FAIL: dtfreq rep78, cross(make) should have failed."
    local failed_tests "`failed_tests' 8"
}

*------------------------------------------------------------------------
* 4.  Binary option domain tests
*------------------------------------------------------------------------

* 4.1 Binary on binary variable (foreign) - should pass
local ++total_tests
sysuse auto, clear
dtrace dtfreq foreign, binary
if _rc {
    di as error "[Test 9] FAIL: dtfreq foreign, binary (should pass)."
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "[Test 9] PASS: dtfreq foreign, binary"
    local passed_tests "`passed_tests' 9"
}

* 4.2 Binary on non-binary variable (rep78) - should return error
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, binary
if _rc == 111 {
    di as result "[Test 10] PASS: Proper error when binary on rep78 (non-binary)."
    local passed_tests "`passed_tests' 10"
}
else if _rc == 198 {
    di as result "[Test 10] PASS: Proper error when binary on rep78 (non-binary) (Stata 16+)"
    local passed_tests "`passed_tests' 10"
}
else if _rc {
    di as error "[Test 10] FAIL: dtfreq rep78, binary returned unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 10"
}
else {
    di as error "[Test 10] FAIL: dtfreq rep78, binary should have failed."
    local failed_tests "`failed_tests' 10"
}

*------------------------------------------------------------------------
* 5.  "using" and "clear" tests with a temp dataset
*------------------------------------------------------------------------

* 5.1 Create and save a small temp dataset
clear
set obs 10
generate var1 = cond(_n <= 5, 0, 1)
generate var2 = cond(_n <= 3, 1, cond(_n <= 7, 2, 3))
save temp_dtfreq, replace

* 5.2 Test dtfreq using "using" on var1 (binary) with clear
local ++total_tests
sysuse auto, clear
dtrace dtfreq var1 using temp_dtfreq.dta, clear binary
if _rc {
    di as error "[Test 11] FAIL: dtfreq var1 using temp_dtfreq.dta, clear, binary"
    local failed_tests "`failed_tests' 11"
}
else {
    di as result "[Test 11] PASS: dtfreq var1 using temp_dtfreq.dta, clear, binary"
    local passed_tests "`passed_tests' 11"
}

* 5.3 Test dtfreq using temp_dtfreq (without clear) - should use in-memory data
local ++total_tests
sysuse auto, clear
dtrace dtfreq var1 using temp_dtfreq.dta
if _rc {
    di as error "[Test 12] FAIL: dtfreq var1 using temp_dtfreq.dta (no clear)"
    local failed_tests "`failed_tests' 12"
}
else {
    di as result "[Test 12] PASS: dtfreq var1 using temp_dtfreq.dta (no clear)"
    local passed_tests "`passed_tests' 12"
}

*------------------------------------------------------------------------
* 6.  save() and REPlace tests
*------------------------------------------------------------------------

* 6.1 Test save() without REPlace (new file)
local ++total_tests
sysuse nlsw88, clear
dtrace dtfreq married, save("ado/ancillary_files/test/dtfreq/dtfreq_output.xlsx")
if _rc {
    di as error "[Test 13] FAIL: dtfreq make, save(dtflux_output.xlsx) failed"
    local failed_tests "`failed_tests' 13"
}
else {
    di as result "[Test 13] PASS: dtfreq make saved to ado/ancillary_files/test/dtfreq/dtfreq_output.xlsx"
    local passed_tests "`passed_tests' 13"
}

* 6.2 Test save() with REPlace
local ++total_tests
sysuse nlsw88, clear
    dtrace dtfreq married, save("ado/ancillary_files/test/dtfreq/dtfreq_output.xlsx") replace
if _rc {
    di as error "[Test 14] FAIL: dtfreq make, save(dtflux_output.dta) replace failed"
    local failed_tests "`failed_tests' 14"
}
else {
    di as result "[Test 14] PASS: dtfreq make, save(... ) replace"
    local passed_tests "`passed_tests' 14"
}

*------------------------------------------------------------------------
* 7.  Excel export tests (_toexcel)
*------------------------------------------------------------------------

* 7.1 Basic Excel export without custom options
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) save("dtfreq_excel_test.dta") excel("")
if _rc {
    di as error "[Test 15] FAIL: dtfreq rep78, cross(foreign) save(... ) excel(\"\")"
    local failed_tests "`failed_tests' 15"
}
else {
    di as result "[Test 15] PASS: Exported to Excel default sheet"
    local passed_tests "`passed_tests' 15"
}

* 7.2 Excel export with custom sheet name
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) save("dtfreq_excel_sheet.dta") ///
    excel(sheet("CustomSheet", replace) firstrow(varlabels))
if _rc {
    di as error "[Test 16] FAIL: dtfreq rep78, cross(foreign) with custom sheet name"
    local failed_tests "`failed_tests' 16"
}
else {
    di as result "[Test 16] PASS: Exported to Excel with CustomSheet"
    local passed_tests "`passed_tests' 16"
}

*------------------------------------------------------------------------
* 8.  STATS / TYPE boundary checks
*------------------------------------------------------------------------

* 8.1 STATS() without cross() should error
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, STATs(cell)
if _rc == 198 {
    di as result "[Test 17] PASS: Proper error when STATs(cell) without cross()"
    local passed_tests "`passed_tests' 17"
}
else if _rc {
    di as error "[Test 17] FAIL: dtfreq rep78, STATs(cell) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 17"
}
else {
    di as error "[Test 17] FAIL: dtfreq rep78, STATs(cell) should have failed"
    local failed_tests "`failed_tests' 17"
}

* 8.2 TYPE() without valid values should error
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, TYPE(invalid)
if _rc == 198 {
    di as result "[Test 18] PASS: Proper error when TYPE(invalid)"
    local passed_tests "`passed_tests' 18"
}
else if _rc {
    di as error "[Test 18] FAIL: dtfreq rep78, TYPE(invalid) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 18"
}
else {
    di as error "[Test 18] FAIL: dtfreq rep78, TYPE(invalid) should have failed"
    local failed_tests "`failed_tests' 18"
}

* 8.3 Duplication in STATS() should error
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) STATs(row row)
if _rc == 198 {
    di as result "[Test 19] PASS: Proper error for duplicate STATS(row row)"
    local passed_tests "`passed_tests' 19"
}
else if _rc {
    di as error "[Test 19] FAIL: dtfreq rep78, cross(foreign) STATs(row row) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 19"
}
else {
    di as error "[Test 19] FAIL: dtfreq rep78, cross(foreign) STATs(row row) should have failed"
    local failed_tests "`failed_tests' 19"
}

* 8.4 Duplication in TYPE() should error
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) STATs(row) TYPE(prop prop)
if _rc == 198 {
    di as result "[Test 20] PASS: Proper error for duplicate TYPE(prop prop)"
    local passed_tests "`passed_tests' 20"
}
else if _rc {
    di as error "[Test 20] FAIL: dtfreq rep78, cross(foreign) TYPE(prop prop) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 20"
}
else {
    di as error "[Test 20] FAIL: dtfreq rep78, cross(foreign) TYPE(prop prop) should have failed"
    local failed_tests "`failed_tests' 20"
}

*------------------------------------------------------------------------
* 9.  CROSS-validation: ensure by() != cross()
*------------------------------------------------------------------------

local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, by(rep78) cross(rep78)
if _rc == 198 {
    di as result "[Test 21] PASS: Proper error when by(rep78) co-specified with cross(rep78)"
    local passed_tests "`passed_tests' 21"
}
else if _rc {
    di as error "[Test 21] FAIL: dtfreq rep78, by(rep78) cross(rep78) gave unexpected code =`=_rc'"
    local failed_tests "`failed_tests' 21"
}
else {
    di as error "[Test 21] FAIL: dtfreq rep78, by(rep78) cross(rep78) should have failed"
    local failed_tests "`failed_tests' 21"
}

*------------------------------------------------------------------------
* 10.  Clean up temporary files
*------------------------------------------------------------------------

capture dtrace erase temp_dtfreq.dta
capture dtrace erase dtfreq_output.dta
capture dtrace erase dtfreq_excel_test.dta
capture dtrace erase dtfreq_excel_sheet.dta

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
        di as result "Test `test'"
    }
}

if `num_failed' > 0 {
    di _n as error "FAILED TESTS:"
    foreach test in `failed_tests' {
        di as error "Test `test'"
    }
}
else {
    di _n as result "ALL TESTS PASSED!"
}

// Final summary
di _n(2) "=========================================="
di "dtfreq Test Suite 2 Completed"
di "Timestamp: " c(current_date) " " c(current_time)
if `num_failed' == 0 {
    di as result "Status: ALL TESTS PASSED"
}
else {
    di as error "Status: " `num_failed' " TESTS FAILED"
}
di "=========================================="

log close
exit
