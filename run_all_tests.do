* run_all_tests.do
* Master test runner for dtkit package
* Executes all test files and provides comprehensive summary
* Date: June 28, 2025

version 16
clear all
capture log close
set more off

// Device detection and path setup
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}

// Create master log
capture log close _all
log using ado/ancillary_files/test/log/run_all_tests.log, replace

// Display header
di _n(2) "=========================================="
di "DTKIT MASTER TEST RUNNER"
di "=========================================="
di "Timestamp: " c(current_date) " " c(current_time)
di "Stata version: " c(stata_version)
di "Working directory: " c(pwd)
di "==========================================" _n

// Initialize tracking variables
local test_files ""
local test_files `test_files' "ado/ancillary_files/test/dtfreq/dtfreq_test1.do"
local test_files `test_files' "ado/ancillary_files/test/dtfreq/dtfreq_test2.do"
local test_files `test_files' "ado/ancillary_files/test/dtmeta/dtmeta_test1.do"
local test_files `test_files' "ado/ancillary_files/test/dtmeta/dtmeta_test2.do"
local test_files `test_files' "ado/ancillary_files/test/dtstat/dtstat_test1.do"
local test_files `test_files' "ado/ancillary_files/test/dtstat/dtstat_test2.do"

local total_files: word count `test_files'
local completed_files 0
local passed_files ""
local failed_files ""
local failed_count 0

di "Running `total_files' test files..." _n

// Execute each test file
foreach test_file in `test_files' {
    local ++completed_files
    local test_name: word `completed_files' of `test_files'
    local test_name = subinstr("`test_name'", "ado/ancillary_files/test/", "", .)
    local test_name = subinstr("`test_name'", "/", "_", .)
    local test_name = subinstr("`test_name'", ".do", "", .)
    
    local progress = round((`completed_files' / `total_files') * 100, 0.1)
    
    di "=========================================="
    di "[`completed_files'/`total_files'] (`progress'%) Running: `test_file'"
    di "=========================================="
    
    // Execute the test file and capture return code
    capture noisily do "`test_file'"
    local test_rc = _rc
    
    if `test_rc' == 0 {
        di _n as result "✓ TEST PASSED: `test_name'"
        local passed_files "`passed_files' `test_name'"
    }
    else {
        di _n as error "✗ TEST FAILED: `test_name' (return code: `test_rc')"
        local failed_files "`failed_files' `test_name'"
        local ++failed_count
    }
    
    di _n
}

// Generate comprehensive summary
di _n(2) "=========================================="
di "MASTER TEST SUMMARY"
di "=========================================="
di "Execution completed: " c(current_date) " " c(current_time)
di "Total test files: `total_files'"

local passed_count: word count `passed_files'
di "Passed: `passed_count' (" %4.1f (`passed_count'/`total_files'*100) "%)"
di "Failed: `failed_count' (" %4.1f (`failed_count'/`total_files'*100) "%)"

if `passed_count' > 0 {
    di _n as result "PASSED TESTS:"
    foreach test in `passed_files' {
        di as result "  ✓ `test'"
    }
}

if `failed_count' > 0 {
    di _n as error "FAILED TESTS:"
    foreach test in `failed_files' {
        di as error "  ✗ `test'"
    }
    di _n as error "Check individual log files for detailed error information."
}
else {
    di _n as result "🎉 ALL TESTS PASSED SUCCESSFULLY!"
}

// Performance summary
di _n "=========================================="
di "PERFORMANCE SUMMARY"
di "=========================================="
di "Expected total individual tests: 106"
di "  - dtfreq: 46 tests (dtfreq_test1: 25, dtfreq_test2: 21)"
di "  - dtmeta: 18 tests (dtmeta_test1: 7, dtmeta_test2: 11)"
di "  - dtstat: 42 tests (dtstat_test1: 23, dtstat_test2: 19)"

// Cleanup instructions
di _n "=========================================="
di "CLEANUP INSTRUCTIONS"
di "=========================================="
di "To clean up log files created during testing, run:"
di `"  . do cleanup_test_logs.do"'
di "Or manually remove files from project root if created by /e flag usage."

// Final status
di _n "=========================================="
if `failed_count' == 0 {
    di as result "OVERALL STATUS: SUCCESS - All test files completed without errors"
    di as result "The dtkit package test suite is fully operational."
}
else {
    di as error "OVERALL STATUS: FAILURE - `failed_count' test file(s) failed"
    di as error "Review failed tests before proceeding with development."
}
di "=========================================="

log close

// Return appropriate exit code
if `failed_count' > 0 {
    exit `failed_count'
}
else {
    exit 0
} 