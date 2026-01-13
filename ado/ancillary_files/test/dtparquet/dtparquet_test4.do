*! dtparquet_test4.do - Phase 4 Streaming & Data Signature Verification
* Date: Jan 13, 2026

version 16
clear all
clear frames
capture log close

// Set working directory to project root
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using "ado/ancillary_files/test/log/dtparquet_test4.log", replace

// Load programs from ado directory to PLUS for testing
discard
local ado_plus = c(sysdir_plus)
copy "ado/dtparquet.ado" "`ado_plus'd/dtparquet.ado", replace
copy "ado/dtparquet.py"  "`ado_plus'd/dtparquet.py", replace

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

display _newline(2) "=========================================="
display "Starting dtparquet Phase 4 (Streaming) Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Streaming Export & Import with DataSignature Verification
display "=== TEST CASE 1: Streaming Round-trip (DataSignature) ==="
local ++total_tests

// 1. Setup Source Data
sysuse auto, clear
expand 100
label variable mpg "Milage (expanded)"
note: "This is a streaming test dataset."
notes make: "Note on make variable."
label define origin_lab 0 "Domestic (USA)" 1 "Foreign (Imported)", modify
label values foreign origin_lab

// Set baseline datasignature
datasignature set, reset
local baseline = r(datasignature)
display "Baseline DataSignature: `baseline'"

tempfile original_dta
save "`original_dta'", replace

// 2. Test Streaming Export
display "Testing streaming export with small chunksize(1000)..."
dtparquet export "test_stream.parquet" using "`original_dta'", replace chunksize(1000)

// 3. Test Streaming Import
display "Testing streaming import with different chunksize(750)..."
dtparquet import "test_restored.dta" using "test_stream.parquet", replace chunksize(750)

// 4. Verification
use "test_restored.dta", clear
datasignature
local restored = r(datasignature)
display "Restored DataSignature: `restored'"

local err 0
if "`baseline'" != "`restored'" {
    display as error "DataSignature mismatch!"
    local ++err
}

// Check Metadata
local vlab : var label mpg
if "`vlab'" != "Milage (expanded)" {
    display as error "Variable label mismatch!"
    local ++err
}

local l0 : label origin_lab 0
if "`l0'" != "Domestic (USA)" {
    display as error "Value label mismatch!"
    local ++err
}

if `err' == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed"
    local failed_tests "`failed_tests' 1"
}

// Cleanup
capture erase "test_stream.parquet"
capture erase "test_restored.dta"

// Test Summary
display _newline "=========================================="
display "Test Suite Summary"
display "Total tests: `total_tests'"
display "Passed: " wordcount("`passed_tests'")
display "Failed: " wordcount("`failed_tests'")
display "=========================================="

if wordcount("`failed_tests'") > 0 {
    display as error "Failed tests: `failed_tests'"
    log close
    exit 1
}
else {
    display as result "All Phase 4 tests passed!"
    log close
    exit 0
}
