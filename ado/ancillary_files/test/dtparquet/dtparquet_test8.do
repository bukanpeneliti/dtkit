* dtparquet_test8.do
* Stress test suite for crash reproduction and regression
* Date: 24feb2026

version 16
clear all
macro drop _all
set more off

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using "ado/ancillary_files/test/log/dtparquet_test8.log", replace

timer clear 99
timer on 99

// Install local versions
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

// Stress parameters
local setup_iterations 1000
local io_iterations 200

display _newline(2) "=========================================="
display "Starting dtparquet Stress Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "Plugin DLL: `plugin_dll'"
display "==========================================" _newline

// Test Case 1: Repeated plugin setup_check calls
display _newline "=== TEST CASE 1: setup_check stress ==="
timer clear 1
timer on 1
local ++total_tests

local setup_fail_at 0
forvalues i = 1/`setup_iterations' {
    capture noisily plugin call dtparquet_plugin, "setup_check"
    if _rc != 0 {
        local setup_fail_at `i'
        continue, break
    }
}

if `setup_fail_at' == 0 {
    display as result "Test 1 completed successfully"
    display as text "setup_check iterations: `setup_iterations'"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: setup_check failed at iteration `setup_fail_at'"
    local failed_tests "`failed_tests' 1"
}

timer off 1
timer list 1
display as text "Test 1 finished in" as result %6.2f r(t1) "s"

// Test Case 2: Repeated save/use roundtrip
display _newline "=== TEST CASE 2: save/use roundtrip stress ==="
timer clear 2
timer on 2
local ++total_tests

local io_fail_at 0
forvalues i = 1/`io_iterations' {
    quietly clear
    quietly set obs 10000
    quietly gen long id = _n
    quietly gen double x = _n * 1.125 + 7
    quietly gen str20 s = "row_" + string(_n)

    capture noisily dtparquet save "stress8_roundtrip.parquet", replace
    if _rc != 0 {
        local io_fail_at `i'
        continue, break
    }

    capture noisily {
        clear
        dtparquet use using "stress8_roundtrip.parquet"
        assert c(N) == 10000
        assert c(k) == 3
        assert id[1] == 1
        assert id[_N] == 10000
        assert s[1] == "row_1"
    }
    if _rc != 0 {
        local io_fail_at `i'
        continue, break
    }
}

if `io_fail_at' == 0 {
    display as result "Test 2 completed successfully"
    display as text "save/use iterations: `io_iterations'"
    local passed_tests "`passed_tests' 2"
}
else {
    display as error "Test 2 failed: roundtrip failed at iteration `io_fail_at'"
    local failed_tests "`failed_tests' 2"
}

capture erase "stress8_roundtrip.parquet"

timer off 2
timer list 2
display as text "Test 2 finished in" as result %6.2f r(t2) "s"

// Display summary
display _newline(2) "=========================================="
display "Stress Test Summary"
display "Total tests: `total_tests'"
display "Passed: " wordcount("`passed_tests'")
display "Failed: " wordcount("`failed_tests'")
display "=========================================="

timer off 99
timer list 99
display as text "Total suite time:" as result %6.2f r(t99) "s"

if wordcount("`failed_tests'") > 0 {
    display as error "Failed tests: `failed_tests'"
    log close
    exit 1
}
else {
    display as result "All stress tests passed!"
    log close
    exit 0
}
