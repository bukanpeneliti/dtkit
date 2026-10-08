* dtstat_test3.do
* Regression tests for dtstat svy mode ([pw=] with an active svyset)
* Date: October 8, 2026

version 16
clear frames
capture log close
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}
log using ado/ancillary_files/test/log/dtstat_test3.log, replace

// manually drop main program and subroutines
capture program drop dtstat
capture program drop _stats
capture program drop _svyest
capture program drop _svycells
capture program drop _svyunit
capture program drop _formatsvy
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

di _n(2) "=========================================="
di "Starting dtstat Test Suite 3 (svy mode)"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// Synthetic survey sample with a designed-in singleton stratum
clear
set obs 60
gen psu = ceil(_n/5)
gen strata = ceil(psu/3)
replace strata = 5 if psu == 12
gen fwt = 1 + mod(_n, 4)
replace fwt = 0 if _n == 7
replace fwt = . if _n == 13
gen y = mod(_n, 7) + 1
gen x = mod(_n, 5) + 1
gen grp = mod(_n, 2)
gen sub = mod(_n, 3) != 0
label variable y "Outcome Y"
svyset psu [pw=fwt], strata(strata) singleunit(centered)

// Test 1: svy mean and total, singleton strata, zero and missing weights
di _n "=== TEST 1: svy mean and total ==="
local ++total_tests
capture noisily {
    quietly svy: mean y
    local b_mean = e(b)[1,1]
    local se_mean = sqrt(e(V)[1,1])
    local n_mean = e(N)
    local npop_mean = e(N_pop)
    local df_mean = e(df_r)
    assert e(N_strata) == 5
    assert e(N_psu) == 12
    assert e(df_r) == e(N_psu) - e(N_strata)
    assert `npop_mean' == 144
    quietly svy: total y
    local b_tot = e(b)[1,1]
    local se_tot = sqrt(e(V)[1,1])
    local n_tot = e(N)
    local npop_tot = e(N_pop)
    local df_tot = e(df_r)

    dtstat y [pw=fwt], stats(mean total) df(svyt1)
    frame svyt1 {
        quietly count
        assert r(N) == 2
        quietly summarize estimate if stat == "mean"
        assert reldif(r(mean), `b_mean') < 1e-10
        quietly summarize se if stat == "mean"
        assert reldif(r(mean), `se_mean') < 1e-10
        quietly summarize n_unw if stat == "mean"
        assert r(mean) == `n_mean'
        quietly summarize n_w if stat == "mean"
        assert reldif(r(mean), `npop_mean') < 1e-10
        quietly summarize df if stat == "mean"
        assert r(mean) == `df_mean'
        quietly summarize estimate if stat == "total"
        assert reldif(r(mean), `b_tot') < 1e-10
        quietly summarize se if stat == "total"
        assert reldif(r(mean), `se_tot') < 1e-10
        quietly summarize n_unw if stat == "total"
        assert r(mean) == `n_tot'
        quietly summarize n_w if stat == "total"
        assert reldif(r(mean), `npop_tot') < 1e-10
        quietly summarize df if stat == "total"
        assert r(mean) == `df_tot'
        assert varname == "y"
        assert varlab == "Outcome Y"
        assert subpop == ""
    }
}
if _rc {
    di as error "Test 1 failed with error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    di as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}

// Test 2: subpop() with if-expression and with variable name
di _n "=== TEST 2: subpop() domain estimation ==="
local ++total_tests
capture noisily {
    quietly svy, subpop(if sub): mean y
    local b_sub = e(b)[1,1]
    local se_sub = sqrt(e(V)[1,1])
    local n_sub = e(N_sub)
    local npop_sub = e(N_subpop)
    local df_sub = e(df_r)

    dtstat y [pw=fwt], stats(mean) subpop(if sub) df(svyt2)
    frame svyt2 {
        quietly count
        assert r(N) == 1
        assert subpop == "if sub"
        quietly summarize estimate
        assert reldif(r(mean), `b_sub') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_sub') < 1e-10
        quietly summarize n_unw
        assert r(mean) == `n_sub'
        quietly summarize n_w
        assert reldif(r(mean), `npop_sub') < 1e-10
        quietly summarize df
        assert r(mean) == `df_sub'
    }

    dtstat y [pw=fwt], stats(mean) subpop(sub) df(svyt2b)
    frame svyt2b {
        assert subpop == "sub"
        quietly summarize estimate
        assert reldif(r(mean), `b_sub') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_sub') < 1e-10
    }
}
if _rc {
    di as error "Test 2 failed with error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    di as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}

// Test 3: ratio term y/x
di _n "=== TEST 3: svy ratio ==="
local ++total_tests
capture noisily {
    quietly svy: ratio y/x
    local b_ratio = e(b)[1,1]
    local se_ratio = sqrt(e(V)[1,1])
    local n_ratio = e(N)
    local npop_ratio = e(N_pop)

    dtstat y/x [pw=fwt], stats(ratio) df(svyt3)
    frame svyt3 {
        quietly count
        assert r(N) == 1
        assert varname == "y/x"
        assert stat == "ratio"
        quietly summarize estimate
        assert reldif(r(mean), `b_ratio') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_ratio') < 1e-10
        quietly summarize n_unw
        assert r(mean) == `n_ratio'
        quietly summarize n_w
        assert reldif(r(mean), `npop_ratio') < 1e-10
    }
}
if _rc {
    di as error "Test 3 failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    di as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}

// Test 4: by() groups keep the full design, total row included
di _n "=== TEST 4: by() with [pw=] ==="
local ++total_tests
capture noisily {
    foreach g in 0 1 {
        quietly svy, subpop(if grp == `g'): mean y
        local b_grp`g' = e(b)[1,1]
        local se_grp`g' = sqrt(e(V)[1,1])
        local n_grp`g' = e(N_sub)
        local npop_grp`g' = e(N_subpop)
    }
    quietly svy: mean y
    local b_all = e(b)[1,1]
    local se_all = sqrt(e(V)[1,1])

    dtstat y [pw=fwt], stats(mean) by(grp) df(svyt4)
    frame svyt4 {
        quietly count
        assert r(N) == 3
        quietly summarize estimate if grp == 0
        assert reldif(r(mean), `b_grp0') < 1e-10
        quietly summarize se if grp == 0
        assert reldif(r(mean), `se_grp0') < 1e-10
        quietly summarize n_unw if grp == 0
        assert r(mean) == `n_grp0'
        quietly summarize n_w if grp == 0
        assert reldif(r(mean), `npop_grp0') < 1e-10
        quietly summarize estimate if grp == 1
        assert reldif(r(mean), `b_grp1') < 1e-10
        quietly summarize se if grp == 1
        assert reldif(r(mean), `se_grp1') < 1e-10
        quietly summarize estimate if grp == -1
        assert reldif(r(mean), `b_all') < 1e-10
        quietly summarize se if grp == -1
        assert reldif(r(mean), `se_all') < 1e-10
        local glbl : value label grp
        assert "`glbl'" != ""
        local tot : label `glbl' -1
        assert "`tot'" == "Total"
    }
}
if _rc {
    di as error "Test 4 failed with error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    di as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}

// Test 5: tiny subpop() domain
di _n "=== TEST 5: tiny subpop() domain ==="
local ++total_tests
capture noisily {
    quietly svy, subpop(if _n <= 2): mean y
    local b_tiny = e(b)[1,1]
    local se_tiny = sqrt(e(V)[1,1])
    local n_tiny = e(N_sub)

    dtstat y [pw=fwt], stats(mean) subpop(if _n <= 2) df(svyt5)
    frame svyt5 {
        quietly summarize estimate
        assert reldif(r(mean), `b_tiny') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_tiny') < 1e-10
        quietly summarize n_unw
        assert r(mean) == `n_tiny'
    }
}
if _rc {
    di as error "Test 5 failed with error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    di as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}

// Test 6: empty subpop() domain returns a missing row, not an error
di _n "=== TEST 6: empty subpop() domain ==="
local ++total_tests
capture noisily {
    dtstat y [pw=fwt], stats(mean) subpop(if y > 100) df(svyt6)
    frame svyt6 {
        quietly count
        assert r(N) == 1
        assert missing(estimate)
        assert missing(se)
        assert missing(ci_l)
        assert missing(ci_u)
        assert missing(df)
        assert n_unw == 0
        assert n_w == 0
    }
}
if _rc {
    di as error "Test 6 failed with error " _rc
    local failed_tests "`failed_tests' 6"
}
else {
    di as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}

// Test 7: explicit svy option uses the design weight
di _n "=== TEST 7: explicit svy option ==="
local ++total_tests
capture noisily {
    quietly svy: mean y
    local b_exp = e(b)[1,1]
    local se_exp = sqrt(e(V)[1,1])

    dtstat y, svy stats(mean) df(svyt7)
    frame svyt7 {
        quietly summarize estimate
        assert reldif(r(mean), `b_exp') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_exp') < 1e-10
    }
}
if _rc {
    di as error "Test 7 failed with error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    di as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}

// Test 8: sum maps to the svy total estimator
di _n "=== TEST 8: sum uses the total estimator ==="
local ++total_tests
capture noisily {
    quietly svy: total y
    local b_sum = e(b)[1,1]
    local se_sum = sqrt(e(V)[1,1])

    dtstat y [pw=fwt], stats(sum) df(svyt8)
    frame svyt8 {
        assert stat == "sum"
        quietly summarize estimate
        assert reldif(r(mean), `b_sum') < 1e-10
        quietly summarize se
        assert reldif(r(mean), `se_sum') < 1e-10
    }
}
if _rc {
    di as error "Test 8 failed with error " _rc
    local failed_tests "`failed_tests' 8"
}
else {
    di as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}

// Test 9: legacy option keeps the weighted-collapse output
di _n "=== TEST 9: legacy option ==="
local ++total_tests
capture noisily {
    dtstat y [pw=fwt], stats(mean) legacy df(svyt9)
    frame svyt9 {
        capture confirm variable mean
        local c_mean = _rc
        capture confirm variable estimate
        local c_est = _rc
        assert `c_mean' == 0
        assert `c_est' != 0
    }
}
if _rc {
    di as error "Test 9 failed with error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    di as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}

// Test 10: option validation errors
di _n "=== TEST 10: svy-mode error handling ==="
local ++total_tests
local test10_errors 0
capture noisily dtstat y [pw=fwt], stats(median) df(svyt10a)
if _rc != 198 {
    di as error "Test 10a failed: unsupported statistic not caught (error " _rc ")"
    local ++test10_errors
}
capture noisily dtstat y/x, stats(ratio) df(svyt10b)
if _rc != 198 {
    di as error "Test 10b failed: ratio outside svy mode not caught (error " _rc ")"
    local ++test10_errors
}
capture noisily dtstat y, stats(mean) subpop(if sub) df(svyt10c)
if _rc != 198 {
    di as error "Test 10c failed: subpop outside svy mode not caught (error " _rc ")"
    local ++test10_errors
}
capture noisily dtstat y [pw=x], stats(mean) df(svyt10d)
if _rc != 198 {
    di as error "Test 10d failed: weight mismatch not caught (error " _rc ")"
    local ++test10_errors
}
capture noisily dtstat y [pw=fwt], stats(mean) subpop(if grp == "0") df(svyt10e)
if _rc != 198 {
    di as error "Test 10e failed: string subpop condition not caught (error " _rc ")"
    local ++test10_errors
}
if `test10_errors' > 0 {
    local failed_tests "`failed_tests' 10"
}
else {
    di as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}

// Cleanup
frame change default
capture frame drop svyt1 svyt2 svyt2b svyt3 svyt4 svyt5 svyt6 svyt7 svyt8 svyt9
capture frame drop _df

// Test summary
di _n(2) "=========================================="
di "TEST SUMMARY"
di "=========================================="

local num_passed: word count `passed_tests'
local num_failed: word count `failed_tests'

di as text "Total tests run: " as result `total_tests'
di as text "Tests passed: " as result `num_passed'
di as text "Tests failed: " as result `num_failed'

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
di "dtstat Test Suite 3 Completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "Check output above for any errors"
di "=========================================="

log close
