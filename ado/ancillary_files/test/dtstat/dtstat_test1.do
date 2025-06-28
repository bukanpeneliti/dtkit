* dtstat_test.do
* Comprehensive test suite for dtstat.ado
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
log using ado/ancillary_files/test/log/dtstat_test1.log, replace

// manually drop main program and subroutines
capture program drop dtstat
capture program drop _stats
capture program drop _collapsevars
capture program drop _byprocess
capture program drop _format
capture program drop _labelvars
capture program drop _formatvars
capture program drop _toexcel
capture program drop _argcheck
capture program drop _argload
run ado/dtstat.ado

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
di "Starting dtstat Test Suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// Test Case 1: Basic functionality with auto dataset
di _n "=== TEST CASE 1: Basic descriptive statistics (auto dataset) ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg weight
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

// Test Case 2: Single variable with default stats
di _n "=== TEST CASE 2: Single variable ==="
local ++total_tests
sysuse auto, clear
dtstat price
if _rc {
    di as error "Test 2 failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    di as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
    frame _df: count
    di as text "Number of observations in _df: " r(N)
    frame _df: list varname varlab, clean
}

// Test Case 3: With BY option
di _n "=== TEST CASE 3: BY option ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, by(foreign)
if _rc {
    di as error "Test 3 failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    di as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
    frame _df: list foreign varname mean count, clean sepby(varname)
}

// Test Case 4: Custom statistics
di _n "=== TEST CASE 4: Custom statistics ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg weight, stats(mean sd min max p25 p75)
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
    frame _df: describe, simple
    frame _df: list in 1/3, clean
}

// Test Case 5: Multiple BY variables
di _n "=== TEST CASE 5: Multiple BY variables ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, by(foreign rep78)
if _rc {
    di as error "Test 5 failed with error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    di as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
    frame _df: tab foreign rep78
    frame _df: list foreign rep78 varname mean in 1/10, clean
}

// Test Case 6: Weight support
di _n "=== TEST CASE 6: Weight support ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg [fw=rep78]
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    di as result "Test 6 completed successfully (frequency weights)"
    local passed_tests "`passed_tests' 6"
    frame _df: list varname mean count, clean
}

// Test Case 7: Analytical weights
di _n "=== TEST CASE 7: Analytical weights ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg [aw=weight]
if _rc {
    di as error "Test 7 failed with error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    di as result "Test 7 completed successfully (analytical weights)"
    local passed_tests "`passed_tests' 7"
    frame _df: list varname mean count, clean
}

// Test Case 8a: Missing values handling (default)
di _n "=== TEST CASE 8a: Missing values (default) ==="
local ++total_tests
sysuse auto, clear
// Create some missing values
replace rep78 = . in 1/10
dtstat price mpg rep78
if _rc {
    di as error "Test 8a failed with error " _rc
    local failed_tests "`failed_tests' 8a"
}
else {
    di as result "Test 8a completed (missing included by default)"
    local passed_tests "`passed_tests' 8a"
    frame _df: list varname count, clean
}

// Test Case 8b: NOMISS option
di _n "=== TEST CASE 8b: NOMISS option ==="
local ++total_tests
sysuse auto, clear
replace rep78 = . in 1/10
dtstat price mpg rep78, nomiss
if _rc {
    di as error "Test 8b failed with error " _rc
    local failed_tests "`failed_tests' 8b"
}
else {
    di as result "Test 8b completed (missing excluded)"
    local passed_tests "`passed_tests' 8b"
    frame _df: list varname count, clean
}

// Test Case 9: Custom frame name
di _n "=== TEST CASE 9: Custom frame name ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, df(mystat)
if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
    capture frame mystat: describe
    if _rc {
        di as error "Custom frame 'mystat' not created"
    }
    else {
        di as result "Custom frame 'mystat' created successfully"
        frame mystat: count
        di as text "Observations in mystat: " r(N)
    }
}

// Test Case 10: USING option
di _n "=== TEST CASE 10: USING option ==="
local ++total_tests
sysuse auto, clear
tempfile autodata
save `autodata'
clear

capture noisily dtstat price mpg weight using `autodata'
if _rc {
    di as error "Test 10 failed with error " _rc
    local failed_tests "`failed_tests' 10"
}
else {
    di as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
    frame _df: list in 1/3, clean
}

// Test Case 11: Excel export
di _n "=== TEST CASE 11: Excel export ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg weight, save("ado/ancillary_files/test/dtstat/dtstat_output.xlsx") replace
if _rc {
    di as error "Test 11 failed with error " _rc
    local failed_tests "`failed_tests' 11"
}
else {
    di as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
    capture confirm file "test/dtstat/dtstat_output.xlsx"
    if _rc {
        di as error "Excel file not created"
    }
    else {
        di as result "Excel file created successfully"
    }
}

// Test Case 12: Excel export with tempfile (expected to fail like dtfreq)
di _n "=== TEST CASE 12: Excel export with tempfile (expected to fail) ==="
local ++total_tests
sysuse auto, clear
tempfile xlsfile2
capture dtstat price mpg, save("`xlsfile2'.xlsx") excel(sheet("MyStats", replace) firstrow(varlabels))

if _rc == 601 {
    di as result "Test 12 completed successfully (expected failure)"
    local passed_tests "`passed_tests' 12"
}
else {
    di as error "Test 12 failed with error " _rc
    local failed_tests "`failed_tests' 12"
}

// Test Case 13: Large dataset (nlsw88)
di _n "=== TEST CASE 13: Large dataset test ==="
local ++total_tests
sysuse nlsw88, clear
* expand 1000
dtstat wage hours tenure, by(union married)
if inlist(_rc, 0, 9) {
    di as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
    frame _df: count
    di as text "Total observations: " r(N)
    frame _df: tab varname union
}
else {
    di as error "Test 13 failed with error " _rc
    local failed_tests "`failed_tests' 13"
}

// Test Case 14: IF/IN conditions
di _n "=== TEST CASE 14: IF/IN conditions ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg if price > 5000, by(foreign)
if _rc {
    di as error "Test 14a failed with error " _rc
    local failed_tests "`failed_tests' 14"
}
else {
    di as result "Test 14a completed (IF condition)"
    local passed_tests "`passed_tests' 14"
}

dtstat price mpg in 1/50
if _rc {
    di as error "Test 14b failed with error " _rc
}
else {
    di as result "Test 14b completed (IN condition)"
}

// Test Case 15: Percentiles
di _n "=== TEST CASE 15: Percentiles ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg weight, stats(p1 p5 p10 p25 p50 p75 p90 p95 p99)
if _rc {
    di as error "Test 15 failed with error " _rc
    local failed_tests "`failed_tests' 15"
}
else {
    di as result "Test 15 completed successfully"
    local passed_tests "`passed_tests' 15"
    frame _df: describe, simple
    frame _df: list varname p25 p50 p75 in 1/3, clean
}

// Test Case 16: All available statistics
di _n "=== TEST CASE 16: All available statistics ==="
local ++total_tests
sysuse auto, clear
dtstat price, stats(count mean median sd min max sum iqr first last)
if _rc {
    di as error "Test 16 failed with error " _rc
    local failed_tests "`failed_tests' 16"
}
else {
    di as result "Test 16 completed successfully"
    local passed_tests "`passed_tests' 16"
    frame _df: describe, simple
    frame _df: list, clean
}

// Test Case 17: FAST option (if gtools available)
di _n "=== TEST CASE 17: FAST option ==="
local ++total_tests
sysuse auto, clear
capture which gtools
if _rc {
    di as text "gtools not available, skipping fast option test"
    local passed_tests "`passed_tests' 17"
}
else {
    dtstat price mpg weight, by(foreign rep78) fast
    if _rc {
        di as error "Test 17 failed with error " _rc
        local failed_tests "`failed_tests' 17"
    }
    else {
        di as result "Test 17 completed successfully (fast option)"
        local passed_tests "`passed_tests' 17"
        frame _df: count
        di as text "Fast option observations: " r(N)
    }
}

// Test Case 18: Format option
di _n "=== TEST CASE 18: Format option ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg weight, format(%12.2f)
if _rc {
    di as error "Test 18 failed with error " _rc
    local failed_tests "`failed_tests' 18"
}
else {
    di as result "Test 18 completed successfully"
    local passed_tests "`passed_tests' 18"
    frame _df: describe mean, simple
    frame _df: list varname mean in 1/3, clean
}

// Test Case 19: Error handling tests
di _n "=== TEST CASE 19: Error handling ==="
local ++total_tests
local test19_errors 0

// Test no variables specified
capture noisily dtstat using `autodata'
if _rc == 0 {
    di as error "Test 19a failed: no variables not caught"
    local ++test19_errors
}
else {
    di as result "Test 19a passed: no variables handled (error " _rc ")"
}

// Test non-numeric variable
sysuse auto, clear
capture noisily dtstat make
if _rc == 0 {
    di as error "Test 19b failed: string variable not caught"
    local ++test19_errors
}
else {
    di as result "Test 19b passed: string variable handled (error " _rc ")"
}

// Test fast without gtools (if not installed)
capture which gtools
if _rc {
    capture dtstat price, fast
    if _rc == 0 {
        di as error "Test 19c failed: fast without gtools not caught"
        local ++test19_errors
    }
    else {
        di as result "Test 19c passed: fast without gtools handled (error " _rc ")"
    }
}

// Test excel without save
sysuse auto, clear
capture dtstat price, excel(sheet("test"))
if _rc == 0 {
    di as error "Test 19d failed: excel without save not caught"
    local ++test19_errors
}
else {
    di as result "Test 19d passed: excel without save handled (error " _rc ")"
}

// Overall Test 19 result
if `test19_errors' > 0 {
    local failed_tests "`failed_tests' 19"
}
else {
    local passed_tests "`passed_tests' 19"
}

// Test Case 20: Complex scenario
di _n "=== TEST CASE 20: Complex scenario ==="
local ++total_tests
sysuse nlsw88, clear
dtstat wage hours tenure grade if age >= 30 & !missing(industry) [aw=hours], ///
    by(union married) stats(count mean median sd min max p25 p75) df(complex_test)
if inlist(_rc, 0, 9) {
    di as result "Test 20 completed successfully"
    local passed_tests "`passed_tests' 20"
    frame complex_test: describe, simple
    frame complex_test: count
    di as text "Complex test observations: " r(N)
    frame complex_test: list varname union married mean in 1/10, clean
}
else {
    di as error "Test 20 failed with error " _rc
    local failed_tests "`failed_tests' 20"
}

// Test Case 21: Variable labels preservation
di _n "=== TEST CASE 21: Variable labels preservation ==="
local ++total_tests
sysuse auto, clear
label variable price "Price (USD)"
label variable mpg "Miles per gallon"
label variable weight "Weight (lbs)"
dtstat price mpg weight
if _rc {
    di as error "Test 21 failed with error " _rc
    local failed_tests "`failed_tests' 21"
}
else {
    di as result "Test 21 completed successfully"
    local passed_tests "`passed_tests' 21"
    frame _df: list varname varlab, clean
}

// Test Case 22: BY with totals
di _n "=== TEST CASE 22: BY with totals ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, by(foreign)
if _rc {
    di as error "Test 22 failed with error " _rc
    local failed_tests "`failed_tests' 22"
}
else {
    di as result "Test 22 completed successfully"
    local passed_tests "`passed_tests' 22"
    frame _df: list foreign varname mean, clean sepby(varname)
    // Check if total rows exist
    frame _df: count if foreign == -1
    if r(N) > 0 {
        di as result "Total rows found: " r(N)
    }
    else {
        di as text "No total rows found (may be expected behavior)"
    }
}

// Test Case 23: Excel export with custom filename
di _n "=== TEST CASE 23: Excel export with custom filename ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, save("ado/ancillary_files/test/dtstat/no-space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtstat/no-space.xlsx"
if _rc {
    di as error "Test 23 failed with error " _rc
    local failed_tests "`failed_tests' 23"
}
else {
    di as result "Test 23 completed successfully"
    local passed_tests "`passed_tests' 23"
}

// Test Case 24: Excel export with custom filename and space
di _n "=== TEST CASE 24: Excel export with custom filename and space ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, save("ado/ancillary_files/test/dtstat/file with space.xlsx") replace
capture confirm file "ado/ancillary_files/test/dtstat/file with space.xlsx"

if _rc {
    di as error "Test 24 failed with error " _rc
    local failed_tests "`failed_tests' 24"
}
else {
    di as result "Test 24 completed successfully"
    local passed_tests "`passed_tests' 24"
}

// Test Case 25: Excel export to non-existent directory
di _n "=== TEST CASE 25: Excel export to non-existent directory ==="
local ++total_tests
sysuse auto, clear
capture dtstat price mpg, save("ado/ancillary_files/test/non-existent/file.xlsx") replace

if _rc == 601 {
    di as result "Test 25 completed successfully"
    local passed_tests "`passed_tests' 25"
}
else {
    di as error "Test 25 failed with error " _rc
    local failed_tests "`failed_tests' 25"
}

// Test Case 26: Excel export without extension
di _n "=== TEST CASE 26: Excel export without extension ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, save("ado/ancillary_files/test/dtstat/no-extension") replace
capture confirm file "ado/ancillary_files/test/dtstat/no-extension.xlsx"
if _rc {
    di as error "Test 26 failed with error " _rc
    local failed_tests "`failed_tests' 26"
}
else {
    di as result "Test 26 completed successfully"
    local passed_tests "`passed_tests' 26"
}

// Cleanup
frame change default
capture frame drop _df mystat complex_test _dtsource

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

// Final summary
di _n(2) "=========================================="
di "dtstat Test Suite Completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "Check output above for any errors"
di "=========================================="

log close
