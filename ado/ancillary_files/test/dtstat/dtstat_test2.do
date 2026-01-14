* dtstat_test2.do
* Comprehensive test suite for dtstat.ado (structure mirrors test/dtfreq/_test2.do)
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
log using ado/ancillary_files/test/log/dtstat_test2.log, replace

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
    set trace on
    set tracedepth 2
    capture noisily `0'
    set trace off
end

di _n(2) "=========================================="
di "Starting dtstat Test Suite 2"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// Test 1: Basic stats, auto dataset
di _n "=== TEST 1: Basic stats ==="
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
    frame _df: list, clean noobs
}

// Test 2: Single variable
di _n "=== TEST 2: Single variable ==="
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
    frame _df: list, clean
}

// Test 3: BY option
di _n "=== TEST 3: BY option ==="
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
    frame _df: list foreign varname mean, clean sepby(varname)
}

// Test 4: Custom stats
di _n "=== TEST 4: Custom stats ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, stats(mean sd min max)
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
    frame _df: list, clean
}

// Test 5: Multiple BY variables
di _n "=== TEST 5: Multiple BY variables ==="
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
    frame _df: list foreign rep78 varname mean, clean
}

// Test 6: Frequency weights
di _n "=== TEST 6: Frequency weights ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg [fw=rep78]
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    di as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
    frame _df: list varname mean count, clean
}

// Test 7: Analytical weights
di _n "=== TEST 7: Analytical weights ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg [aw=weight]
if _rc {
    di as error "Test 7 failed with error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    di as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
    frame _df: list varname mean count, clean
}

// Test 8: Missing values (default)
di _n "=== TEST 8: Missing values (default) ==="
local ++total_tests
sysuse auto, clear
replace rep78 = . in 1/10
dtstat price mpg rep78
if _rc {
    di as error "Test 8 failed with error " _rc
    local failed_tests "`failed_tests' 8"
}
else {
    di as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
    frame _df: list varname count, clean
}

// Test 9: NOMISS option
di _n "=== TEST 9: NOMISS option ==="
local ++total_tests
sysuse auto, clear
replace rep78 = . in 1/10
dtstat price mpg rep78, nomiss
if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
    frame _df: list varname count, clean
}

// Test 10: Custom frame name
di _n "=== TEST 10: Custom frame name ==="
local ++total_tests
sysuse auto, clear
dtstat price mpg, df(mystat)
if _rc {
    di as error "Test 10 failed with error " _rc
    local failed_tests "`failed_tests' 10"
}
else {
    di as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
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

// Test 11: USING option
di _n "=== TEST 11: USING option ==="
local ++total_tests
sysuse auto, clear
tempfile autodata
save `autodata'
clear
capture noisily dtstat price mpg weight using `autodata'
if _rc {
    di as error "Test 11 failed with error " _rc
    local failed_tests "`failed_tests' 11"
}
else {
    di as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
    frame _df: list in 1/3, clean
}

// Test 12: Excel export
di _n "=== TEST 12: Excel export ==="
local ++total_tests
sysuse auto, clear
tempfile xlsfile
dtstat price mpg weight, save("`xlsfile'.xlsx")
if _rc {
    di as error "Test 12 failed with error " _rc
    local failed_tests "`failed_tests' 12"
}
else {
    di as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
    capture confirm file "`xlsfile'.xlsx"
    if _rc {
        di as error "Excel file not created"
    }
    else {
        di as result "Excel file created successfully"
    }
}

// Test 13: Large dataset (nlsw88)
di _n "=== TEST 13: Large dataset ==="
local ++total_tests
sysuse nlsw88, clear
dtstat wage hours tenure, by(union married)
if _rc {
    di as error "Test 13 failed with error " _rc
    local failed_tests "`failed_tests' 13"
}
else {
    di as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
    frame _df: count
    di as text "Total observations: " r(N)
    frame _df: tab varname union
}

// Test 14: IF/IN conditions
di _n "=== TEST 14: IF/IN conditions ==="
local ++total_tests
sysuse auto, clear
local test14_errors 0
dtstat price mpg if price > 5000, by(foreign)
if _rc {
    di as error "Test 14a failed with error " _rc
    local ++test14_errors
}
else {
    di as result "Test 14a completed successfully"
}
dtstat price mpg in 1/50
if _rc {
    di as error "Test 14b failed with error " _rc
    local ++test14_errors
}
else {
    di as result "Test 14b completed successfully"
}
if `test14_errors' > 0 {
    local failed_tests "`failed_tests' 14"
}
else {
    local passed_tests "`passed_tests' 14"
}

// Test 15: Percentiles
di _n "=== TEST 15: Percentiles ==="
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
    frame _df: list varname p25 p50 p75 in 1/3, clean
}

// Test 16: All available statistics
di _n "=== TEST 16: All available statistics ==="
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
    frame _df: list, clean
}

// Test 17: FAST option (if gtools available)
di _n "=== TEST 17: FAST option ==="
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

// Test 18: Format option
di _n "=== TEST 18: Format option ==="
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
    frame _df: list varname mean in 1/3, clean
}

// Test 19: Error handling
di _n "=== TEST 19: Error handling ==="
local ++total_tests
local test19_errors 0
capture noisily dtstat using `autodata'
if _rc == 0 {
    di as error "Test 19a failed: no variables not caught"
    local ++test19_errors
}
else {
    di as result "Test 19a passed: no variables handled (error " _rc ")"
}
sysuse auto, clear
capture noisily dtstat make
if _rc == 0 {
    di as error "Test 19b failed: string variable not caught"
    local ++test19_errors
}
else {
    di as result "Test 19b passed: string variable handled (error " _rc ")"
}
capture which gtools
if _rc {
    dtstat price, fast
    if _rc == 0 {
        di as error "Test 19c failed: fast without gtools not caught"
        local ++test19_errors
    }
    else {
        di as result "Test 19c passed: fast without gtools handled (error " _rc ")"
    }
}
sysuse auto, clear
dtstat price, excel(sheet("test"))
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

di _n(2) "=========================================="
di "dtstat Test Suite 2 Completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "Check output above for any errors"
di "=========================================="

log close
