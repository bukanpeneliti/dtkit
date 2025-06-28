* dtfreq_test.do
* Comprehensive test suite for dtfreq.ado
* Date: June 1, 2025

version 16
clear frames
capture log close
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}
log using ado/ancillary_files/test/log/dtfreq_test1.log, replace

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

// Display test header
di _n(2) "=========================================="
di "Starting dtfreq Test Suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// Test Case 1: Basic functionality with auto dataset (one-way)
di _n "=== TEST CASE 1: Basic one-way frequency (auto dataset) ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78
if _rc {
    di as error "Test 1 failed with error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    di as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
    
    // Check if frame was created
    capture frame _df: describe
    if _rc {
        di as error "Frame _df not created"
    }
    else {
        di as result "Frame _df created successfully"
        frame _df: list, clean noobs
    }
}

// Test Case 2: Multiple variables
di _n "=== TEST CASE 2: Multiple variables ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78 foreign
if _rc {
    di as error "Test 2 failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    di as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
    frame _df: count
    di as text "Number of observations in _df: " r(N)
    frame _df: tab varname
}

// Test Case 3: With BY option
di _n "=== TEST CASE 3: BY option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, by(foreign)
if _rc {
    di as error "Test 3 failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    di as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
    frame _df: list varname foreign vallab freq prop, clean sepby(varname)
}

// Test Case 4: CROSS option (two-way tabulation)
di _n "=== TEST CASE 4: CROSS option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign)
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
    frame _df: describe, simple
    frame _df: list in 1/5, clean
}

// Test Case 5: BINARY option
di _n "=== TEST CASE 5: BINARY option ==="
local ++total_tests
sysuse auto, clear
// Create binary variables with consistent labels
label define yesno 0 "No" 1 "Yes"
generate highprice = (price > 6000)
generate domestic = (foreign == 0)
label values highprice yesno
label values domestic yesno

dtrace dtfreq highprice domestic, binary
if _rc {
    di as error "Test 5 failed with error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    di as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
    frame _df: describe, simple
    frame _df: list, clean
}

// Test Case 6: BINARY with CROSS
di _n "=== TEST CASE 6: BINARY with CROSS ==="
local ++total_tests
sysuse auto, clear
label define yesno 0 "No" 1 "Yes"
generate highprice = (price > 6000)
label values highprice yesno
label values foreign yesno

dtrace dtfreq highprice, cross(foreign) binary
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    di as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
    frame _df: list, clean
}

// Test Case 7: STATS option
di _n "=== TEST CASE 7: STATS option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) stats(row col) 
if _rc {
    di as error "Test 7 failed with error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    di as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
    frame _df: describe, simple
    // Should only have row and col statistics
    frame _df: capture ds *cell*, has(type numeric)
    if "`r(varlist)'" != "" {
        di as error "Cell statistics found when they should be excluded"
    }
    else {
        di as result "Cell statistics correctly excluded"
    }
}

// Test Case 8: TYPE option
di _n "=== TEST CASE 8: TYPE option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, cross(foreign) type(pct)
if _rc {
    di as error "Test 8 failed with error " _rc
    local failed_tests "`failed_tests' 8"
}
else {
    di as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
    frame _df: describe, simple
    // Should only have percentages, not proportions
    frame _df: capture ds *prop*, has(type numeric)
    if "`r(varlist)'" != "" {
        di as error "Proportion variables found when they should be excluded"
    }
    else {
        di as result "Proportion variables correctly excluded"
    }
}

// Test Case 9: Weight support
di _n "=== TEST CASE 9: Weight support ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78 [fw=rep78]
if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "Test 9 completed successfully (frequency weights)"
    local passed_tests "`passed_tests' 9"
    frame _df: list, clean noobs
}

// Test Case 10: Missing values handling
di _n "=== TEST CASE 10: Missing values (default) ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78
if _rc {
    di as error "Test 10a failed with error " _rc
    local failed_tests "`failed_tests' 10a"
}
else {
    di as result "Test 10a completed (missing included by default)"
    local passed_tests "`passed_tests' 10a"
    frame _df: count if missing(vallab)
    di as text "Missing value rows: " r(N)
}

// Test Case 10b: NOMISS option
di _n "=== TEST CASE 10b: NOMISS option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, nomiss
if _rc {
    di as error "Test 10b failed with error " _rc
    local failed_tests "`failed_tests' 10b"
}
else {
    di as result "Test 10b completed (missing excluded)"
    local passed_tests "`passed_tests' 10b"
    frame _df: count if missing(vallab)
    if r(N) > 0 {
        di as error "Missing values found when they should be excluded"
    }
    else {
        di as result "Missing values correctly excluded"
    }
}

// Test Case 11: Custom frame name
di _n "=== TEST CASE 11: Custom frame name ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, df(myfreq)
if _rc {
    di as error "Test 11 failed with error " _rc
    local failed_tests "`failed_tests' 11"
}
else {
    di as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
    capture frame myfreq: describe
    if _rc {
        di as error "Custom frame 'myfreq' not created"
    }
    else {
        di as result "Custom frame 'myfreq' created successfully"
        frame myfreq: count
        di as text "Observations in myfreq: " r(N)
    }
}

// Test Case 12: USING option
di _n "=== TEST CASE 12: USING option ==="
local ++total_tests
sysuse auto, clear
tempfile autodata
save `autodata'
clear

capture noisily dtfreq rep78 using `autodata'

if _rc {
    di as error "Test 12 failed with error " _rc
    local failed_tests "`failed_tests' 12"
}
else {
    di as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
    frame _df: list in 1/5, clean
}

// Test Case 13: Excel export
di _n "=== TEST CASE 13: Excel export ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, save("ado/ancillary_files/test/dtfreq/xlsfile.xlsx") replace
if _rc {
    di as error "Test 13 failed with error " _rc
    local failed_tests "`failed_tests' 13"
}
else {
    di as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
            capture confirm file "ado/ancillary_files/test/dtfreq/xlsfile.xlsx"
    if _rc {
        di as error "Excel file not created"
    }
    else {
        di as result "Excel file created successfully"
    }
}

// Test Case 14: Large dataset (nlsw88)
di _n "=== TEST CASE 14: Large dataset test ==="
local ++total_tests
sysuse nlsw88, clear
dtrace dtfreq industry race, by(union) cross(married)
if _rc {
    di as error "Test 14 failed with error " _rc
    local failed_tests "`failed_tests' 14"
}
else {
    di as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
    frame _df: count
    di as text "Total observations: " r(N)
    frame _df: tab varname union
}

// Test Case 15: IF/IN conditions
di _n "=== TEST CASE 15: IF/IN conditions ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78 if price > 5000, by(foreign)
if _rc {
    di as error "Test 15a failed with error " _rc
    local failed_tests "`failed_tests' 15a"
}
else {
    di as result "Test 15a completed (IF condition)"
    local passed_tests "`passed_tests' 15a"
}

local ++total_tests
dtrace dtfreq rep78 in 1/50
if _rc {
    di as error "Test 15b failed with error " _rc
    local failed_tests "`failed_tests' 15b"
}
else {
    di as result "Test 15b completed (IN condition)"
    local passed_tests "`passed_tests' 15b"
}

// Test Case 16: Error handling tests
di _n "=== TEST CASE 16: Error handling ==="

// Test invalid stats option
local ++total_tests
dtrace dtfreq rep78, stats(invalid)
if _rc == 0 {
    di as error "Test 16a failed: invalid stats option not caught"
    local failed_tests "`failed_tests' 16a"
}
else {
    di as result "Test 16a passed: invalid stats option handled (error " _rc ")"
    local passed_tests "`passed_tests' 16a"
}

// Test stats without cross
local ++total_tests
dtrace dtfreq rep78, stats(row)
if _rc == 0 {
    di as error "Test 16b failed: stats without cross not caught"
    local failed_tests "`failed_tests' 16b"
}
else {
    di as result "Test 16b passed: stats without cross handled (error " _rc ")"
    local passed_tests "`passed_tests' 16b"
}

// Test clear without using
local ++total_tests
clear
dtrace dtfreq rep78, clear
if _rc == 0 {
    di as error "Test 16c failed: clear without using not caught"
    local failed_tests "`failed_tests' 16c"
}
else {
    di as result "Test 16c passed: clear without using handled (error " _rc ")"
    local passed_tests "`passed_tests' 16c"
}

// Test replace without save
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, replace
if _rc == 0 {
    di as error "Test 16d failed: replace without save not caught"
    local failed_tests "`failed_tests' 16d"
}
else {
    di as result "Test 16d passed: replace without save handled (error " _rc ")"
    local passed_tests "`passed_tests' 16d"
}

// Test binary with inconsistent labels
local ++total_tests
sysuse auto, clear
generate test1 = (rep78 > 3)
generate test2 = (rep78 > 3)
label define lab1 0 "No" 1 "Yes"
label define lab2 0 "False" 1 "True"
label values test1 lab1
label values test2 lab2

dtrace dtfreq test1 test2, binary
if _rc == 0 {
    di as error "Test 16e failed: inconsistent binary labels not caught"
    local failed_tests "`failed_tests' 16e"
}
else {
    di as result "Test 16e passed: inconsistent binary labels handled (error " _rc ")"
    local passed_tests "`passed_tests' 16e"
}

// Test Case 17: Format option
di _n "=== TEST CASE 17: Format option ==="
local ++total_tests
sysuse auto, clear
dtrace dtfreq rep78, format(%8.2f)
if _rc {
    di as error "Test 17 failed with error " _rc
    local failed_tests "`failed_tests' 17"
}
else {
    di as result "Test 17 completed successfully"
    local passed_tests "`passed_tests' 17"
    frame _df: describe freq, simple
    frame _df: list freq in 1/3, clean
}

// Test Case 18: Complex scenario
di _n "=== TEST CASE 18: Complex scenario ==="
local ++total_tests
sysuse nlsw88, clear
label define binary_lbl 0 "No" 1 "Yes"
generate college_grad = (grade >= 16)
generate high_wage = (wage > 10)
label values college_grad binary_lbl
label values high_wage binary_lbl

dtrace dtfreq college_grad high_wage if age >= 30 [aw=hours], ///
    by(union) cross(married) binary stats(row col) type(prop pct) df(complex_test)
if _rc {
    di as error "Test 18 failed with error " _rc
    local failed_tests "`failed_tests' 18"
}
else {
    di as result "Test 18 completed successfully"
    local passed_tests "`passed_tests' 18"
    frame complex_test: describe, simple
    frame complex_test: count
    di as text "Complex test observations: " r(N)
}

// Test Case 19: Missing total row
di _n "=== TEST CASE 19: Missing total row ==="
local ++total_tests
sysuse nlsw88, clear
dtrace dtfreq married, cross(south) stats(row col cell) type(prop pct)
frame _df: mdesc
frame _df: assert missing("`r(miss_vars)'") 
if _rc {
    di as error "Test 19 failed with error " _rc
    local failed_tests "`failed_tests' 19"
}
else {
    di as result "Test 19 completed successfully"
    local passed_tests "`passed_tests' 19"
    frame _df: mdesc
}

// Test Case 20: Excel export with custom filename
di _n "=== TEST CASE 20: Excel export with custom filename ==="
local ++total_tests
sysuse auto, clear
    dtfreq rep78, save("ado/ancillary_files/test/dtfreq/no-space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtfreq/no-space.xlsx"
if _rc {
    di as error "Test 20 failed with error " _rc
    local failed_tests "`failed_tests' 20"
}
else {
    di as result "Test 20 completed successfully"
    local passed_tests "`passed_tests' 20"
}

// Test Case 21: Excel export with custom filename and space
di _n "=== TEST CASE 21: Excel export with custom filename and space ==="
local ++total_tests
sysuse auto, clear
    dtrace dtfreq rep78, save("ado/ancillary_files/test/dtfreq/file with space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtfreq/file with space.xlsx"

if _rc {
    di as error "Test 21 failed with error " _rc
    local failed_tests "`failed_tests' 21"
}
else {
    di as result "Test 21 completed successfully"
    local passed_tests "`passed_tests' 21"
}

// Test Case 22: Excel export to non-existent directory
di _n "=== TEST CASE 22: Excel export to non-existent directory ==="
local ++total_tests
sysuse auto, clear
    capture dtfreq rep78, save("ado/ancillary_files/test/non-existent/file.xlsx") replace

if _rc == 601 {
    di as result "Test 22 completed successfully"
    local passed_tests "`passed_tests' 22"
}
else {
    di as error "Test 22 failed with error " _rc
    local failed_tests "`failed_tests' 22"
}

// Test Case 23: Excel export without extension
di _n "=== TEST CASE 23: Excel export without extension ==="
local ++total_tests
sysuse auto, clear
    dtrace dtfreq rep78, save("ado/ancillary_files/test/dtfreq/no-extension") replace
capture confirm file "ado/ancillary_files/test/dtfreq/no-extension.xlsx"
if _rc {
    di as error "Test 23 failed with error " _rc
    local failed_tests "`failed_tests' 23"
}
else {
    di as result "Test 23 completed successfully"
    local passed_tests "`passed_tests' 23"
}

// Cleanup
frame change default
capture frame drop _df myfreq complex_test _dtsource

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
        di as result "  PASS Test `test'"
    }
}

if `num_failed' > 0 {
    di _n as error "FAILED TESTS:"
    foreach test in `failed_tests' {
        di as error "  FAIL Test `test'"
    }
}
else {
    di _n as result "ALL TESTS PASSED!"
}

// Final summary
di _n(2) "=========================================="
di "dtfreq Test Suite Completed"
di "Timestamp: " c(current_date) " " c(current_time)
if `num_failed' == 0 {
    di as result "Status: ALL TESTS PASSED"
}
else {
    di as error "Status: " `num_failed' " TESTS FAILED"
}
di "=========================================="

log close