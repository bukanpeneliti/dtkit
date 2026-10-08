*! dtfreq_test3.do
*! Regression test suite for dtfreq with pweight and subpop() support
*! Date: October 2026

version 16
clear frames
capture log close

capture confirm file "ado/dtfreq.ado"
if _rc != 0 {
    if c(hostname) == "NUXS" {
        cd d:/OneDrive/MyWork/00personal/stata/dtkit
    }
    else {
        cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
    }
}
log using ado/ancillary_files/test/log/dtfreq_test3.log, replace

// Manually drop main program and subroutines
capture program drop dtfreq
capture program drop _xtab
capture program drop _xtab_core
capture program drop _combine_subpop
capture program drop _binreshape
capture program drop _crosstotal
capture program drop _labelvars
capture program drop _toexcel
capture program drop _formatvars
capture program drop _argcheck
capture program drop _argload
capture mata: mata drop _xtab_core_calc()
capture mata: mata drop _xtab_core_calc_svy()
run ado/dtfreq.ado

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
di "Starting dtfreq Survey & Subpop Test Suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// * Generate synthetic survey dataset
// Designed-in features:
// - 3 strata: stratum 1 (PSUs 1,2), stratum 2 (PSUs 3,4), stratum 3 (PSU 5: singleton stratum)
// - singleunit(centered)
// - fwt: positive sampling weights
// - catvar: 3 categories (Low, Medium, High)
// - binvar: binary 0/1 variable
// - subvar: subpopulation indicator
// - groupvar: by/cross grouping variable

clear
set seed 98765
set obs 60

generate strata = 1 in 1/25
replace strata = 2 in 26/50
replace strata = 3 in 51/60

generate psu = .
replace psu = cond(_n <= 12, 1, 2) if strata == 1
replace psu = cond(_n <= 38, 3, 4) if strata == 2
replace psu = 5 if strata == 3

generate double fwt = 10 + runiform() * 5

label define catlbl 1 "Low" 2 "Medium" 3 "High"
generate catvar = 1 + floor(runiform() * 3)
label values catvar catlbl

label define binlbl 0 "No" 1 "Yes"
generate binvar = (catvar >= 2)
label values binvar binlbl

generate subvar = (_n <= 45)
generate groupvar = cond(_n <= 30, 0, 1)

tempfile synthdata
save `synthdata'

// * Test Case 1: One-way survey frequency with [pw=fwt]
di _n "=== TEST CASE 1: Basic one-way survey frequency ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

// Stata svy benchmark
svy: tabulate catvar
matrix b_svy = e(b)
matrix V_svy = e(V)
scalar df_svy = e(df_r)
scalar total_svy = e(total)
scalar n_svy = e(N)

dtrace dtfreq catvar [pw=fwt]
if _rc {
    di as error "Test 1 failed with error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    // Assert results match svy benchmark
    frame _df {
        quietly count
        assert r(N) == 3
        quietly summarize total, meanonly
        assert reldif(r(mean), scalar(total_svy)) < 1e-6
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_svy)
        quietly summarize prop, meanonly
        assert abs(r(sum) - 1.0) < 1e-6
        quietly summarize freq, meanonly
        assert reldif(r(sum), scalar(total_svy)) < 1e-6
        quietly summarize freq_unw, meanonly
        assert r(sum) == scalar(n_svy)
    }
    di as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}

// * Test Case 2: Subpopulation estimation with subpop(if ...)
di _n "=== TEST CASE 2: Subpopulation with subpop(if subvar == 1) ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

svy, subpop(if subvar == 1): tabulate catvar
scalar total_sub_svy = e(N_subpop)
scalar n_sub_svy = e(N_sub)
scalar df_sub_svy = e(df_r)

dtrace dtfreq catvar [pw=fwt], subpop(if subvar == 1)
if _rc {
    di as error "Test 2 failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    frame _df {
        quietly count
        assert r(N) == 3
        quietly summarize total, meanonly
        assert reldif(r(mean), scalar(total_sub_svy)) < 1e-6
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_sub_svy)
        quietly summarize prop, meanonly
        assert abs(r(sum) - 1.0) < 1e-6
        quietly summarize freq, meanonly
        assert reldif(r(sum), scalar(total_sub_svy)) < 1e-6
        quietly summarize freq_unw, meanonly
        assert r(sum) == scalar(n_sub_svy)
    }
    di as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}

// * Test Case 3: Subpopulation with subpop(varname) syntax
di _n "=== TEST CASE 3: Subpopulation with subpop(subvar) ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw=fwt], subpop(subvar)
if _rc {
    di as error "Test 3 failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    frame _df {
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_sub_svy)
        quietly summarize total, meanonly
        assert reldif(r(mean), scalar(total_sub_svy)) < 1e-6
    }
    di as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}

// * Test Case 4: Bare [pw] inherits active svyset weight
di _n "=== TEST CASE 4: Bare [pw] inherits svyset weight ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw]
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    frame _df {
        quietly summarize total, meanonly
        assert reldif(r(mean), scalar(total_svy)) < 1e-6
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_svy)
    }
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}

// * Test Case 5: Survey tabulation with by(groupvar)
di _n "=== TEST CASE 5: Survey tabulation by(groupvar) ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw=fwt], by(groupvar)
if _rc {
    di as error "Test 5 failed with error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    frame _df {
        // Must contain groups 0, 1, and -1 (Total)
        quietly levelsof groupvar, local(g_levels)
        assert "`g_levels'" == "-1 0 1"
        // Total group (-1) must match overall population
        quietly summarize total if groupvar == -1, meanonly
        assert reldif(r(mean), scalar(total_svy)) < 1e-6
        quietly summarize total_unw if groupvar == -1, meanonly
        assert r(mean) == scalar(n_svy)
    }
    di as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}

// * Test Case 6: Two-way cross tabulation with cross(groupvar)
di _n "=== TEST CASE 6: Survey cross-tabulation cross(groupvar) ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw=fwt], cross(groupvar)
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    frame _df {
        // Assert presence of unweighted and weighted columns
        confirm variable freq0 freq1 freq_unw0 freq_unw1 total0 total1 total_unw0 total_unw1 total_all total_all_unw
        quietly summarize total_all, meanonly
        assert reldif(r(mean), scalar(total_svy)) < 1e-6
        quietly summarize total_all_unw, meanonly
        assert r(mean) == scalar(n_svy)
    }
    di as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}

// * Test Case 7: Binary reshaping with [pw=fwt]
di _n "=== TEST CASE 7: Binary reshaping under [pw=fwt] ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq binvar [pw=fwt], binary
if _rc {
    di as error "Test 7 failed with error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    frame _df {
        quietly count
        assert r(N) == 1
        confirm variable freq_no freq_yes freq_unw_no freq_unw_yes prop_no prop_yes se_no se_yes ci_l_no ci_u_no total total_unw
        assert total_unw == scalar(n_svy)
        assert reldif(total, scalar(total_svy)) < 1e-6
        assert (freq_unw_no + freq_unw_yes) == scalar(n_svy)
    }
    di as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}

// * Test Case 8: Zero weights handled correctly
di _n "=== TEST CASE 8: Zero-weight observation ==="
local ++total_tests
use `synthdata', clear
replace fwt = 0 in 1
svyset psu [pw=fwt], strata(strata) singleunit(centered)

svy: tabulate catvar
scalar n_svy8 = e(N)
scalar total_svy8 = e(total)

dtrace dtfreq catvar [pw=fwt]
if _rc {
    di as error "Test 8 failed with error " _rc
    local failed_tests "`failed_tests' 8"
}
else {
    frame _df {
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_svy8)
        quietly summarize total, meanonly
        assert reldif(r(mean), scalar(total_svy8)) < 1e-6
    }
    di as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}

// * Test Case 9: Missing weights excluded correctly
di _n "=== TEST CASE 9: Missing weight observation ==="
local ++total_tests
use `synthdata', clear
replace fwt = . in 1
svyset psu [pw=fwt], strata(strata) singleunit(centered)

svy: tabulate catvar
scalar n_svy9 = e(N)

dtrace dtfreq catvar [pw=fwt]
if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    frame _df {
        quietly summarize total_unw, meanonly
        assert r(mean) == scalar(n_svy9)
        assert r(mean) == 59
    }
    di as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}

// * Test Case 10: Error handling - not svyset
di _n "=== TEST CASE 10: Error when data not svyset ==="
local ++total_tests
use `synthdata', clear
svyset, clear

dtrace dtfreq catvar [pw=fwt]
if _rc == 119 {
    di as result "Test 10 passed: caught not-svyset error (rc 119)"
    local passed_tests "`passed_tests' 10"
}
else {
    di as error "Test 10 failed: expected rc 119, got " _rc
    local failed_tests "`failed_tests' 10"
}

// * Test Case 11: Error handling - subpop() without pw
di _n "=== TEST CASE 11: Error when subpop() specified without pweights ==="
local ++total_tests
use `synthdata', clear

dtrace dtfreq catvar, subpop(if subvar == 1)
if _rc == 198 {
    di as result "Test 11 passed: caught subpop without pw (rc 198)"
    local passed_tests "`passed_tests' 11"
}
else {
    di as error "Test 11 failed: expected rc 198, got " _rc
    local failed_tests "`failed_tests' 11"
}

// * Test Case 12: Error handling - weight variable mismatch
di _n "=== TEST CASE 12: Error when weight variable does not match svyset ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw=groupvar]
if _rc == 198 {
    di as result "Test 12 passed: caught weight mismatch (rc 198)"
    local passed_tests "`passed_tests' 12"
}
else {
    di as error "Test 12 failed: expected rc 198, got " _rc
    local failed_tests "`failed_tests' 12"
}

// * Test Case 13: Error handling - empty subpop domain
di _n "=== TEST CASE 13: Empty subpop domain raises rc 461 ==="
local ++total_tests
use `synthdata', clear
svyset psu [pw=fwt], strata(strata) singleunit(centered)

dtrace dtfreq catvar [pw=fwt], subpop(if strata == 999)
if _rc == 461 {
    di as result "Test 13 passed: caught empty subpop (rc 461)"
    local passed_tests "`passed_tests' 13"
}
else {
    di as error "Test 13 failed: expected rc 461, got " _rc
    local failed_tests "`failed_tests' 13"
}

// Cleanup
frame change default
capture frame drop _df

// Test Summary
di _n(2) "=========================================="
di "TEST SUMMARY"
di "=========================================="

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

di _n(2) "=========================================="
di "dtfreq Survey & Subpop Test Suite Completed"
di "Timestamp: " c(current_date) " " c(current_time)
if `num_failed' == 0 {
    di as result "Status: ALL TESTS PASSED"
}
else {
    di as error "Status: " `num_failed' " TESTS FAILED"
}
di "=========================================="

log close
exit, clear
