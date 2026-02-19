*! dtparquet_test4.do - Phase 4 Streaming & Data Signature Verification
* Date: Jan 13, 2026

version 16
clear all
clear frames
capture log close

// Set working directory to project root
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using "ado/ancillary_files/test/log/dtparquet_test4.log", replace

// Load programs from ado directory
discard
adopath ++ "D:/OneDrive/MyWork/00personal/stata/dtkit/ado"

local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
capture noisily copy "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
if _rc != 0 {
    local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll"
}
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

display _newline(2) "=========================================="
display "Starting dtparquet Phase 4 (Streaming) Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Setup Source Data (used by multiple tests)
sysuse auto, clear
expand 100
label variable mpg "Milage (expanded)"
notes _dta: "This is a streaming test dataset."
notes make: "Note on make variable."
label define origin_lab 0 "Domestic (USA)" 1 "Foreign (Imported)", modify
label values foreign origin_lab

datasignature set, reset
local baseline = r(datasignature)

tempfile original_dta
save "`original_dta'", replace

// Capture expected values for verification
local expected_mpg_label : variable label mpg
local expected_foreign_valuelabel : value label foreign
local expected_l0 : label origin_lab 0
local expected_l1 : label origin_lab 1
local expected_varcount = c(k)
local expected_obs = c(N)

// Capture expected note counts and content
local expected_dta_note_count : char _dta[note0]
local expected_make_note_count : char make[note0]

// Capture note content into locals BEFORE saving
forvalues i = 1/`expected_dta_note_count' {
    local expected_dta_note_`i' : char _dta[note`i']
}
forvalues i = 1/`expected_make_note_count' {
    local expected_make_note_`i' : char make[note`i']
}

// Test Case 1: Streaming Export with Small Chunksize
display _newline "=== TEST CASE 1: Streaming Export (chunksize=1000) ==="
local ++total_tests
dtparquet export "test_stream.parquet" using "`original_dta'", replace chunksize(1000)
if _rc == 0 {
    capture confirm file "test_stream.parquet"
    if _rc == 0 {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: output file not created"
        local failed_tests "`failed_tests' 1"
    }
}
else {
    display as error "Test 1 failed: rc=" _rc
    local failed_tests "`failed_tests' 1"
}

// Test Case 2: Streaming Import with Different Chunksize
display _newline "=== TEST CASE 2: Streaming Import (chunksize=750) ==="
local ++total_tests
dtparquet import "test_restored.dta" using "test_stream.parquet", replace chunksize(750)
if _rc == 0 {
    capture confirm file "test_restored.dta"
    if _rc == 0 {
        display as result "Test 2 completed successfully"
        local passed_tests "`passed_tests' 2"
    }
    else {
        display as error "Test 2 failed: output file not created"
        local failed_tests "`failed_tests' 2"
    }
}
else {
    display as error "Test 2 failed: rc=" _rc
    local failed_tests "`failed_tests' 2"
}

// Test Case 3: DataSignature Verification After Round-trip
display _newline "=== TEST CASE 3: DataSignature Verification ==="
local ++total_tests
use "test_restored.dta", clear
datasignature
local restored = r(datasignature)
if "`baseline'" == "`restored'" {
    display as result "Test 3 completed successfully"
    display "  Baseline:  `baseline'"
    display "  Restored:  `restored'"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: DataSignature mismatch"
    display as error "  Baseline:  `baseline'"
    display as error "  Restored:  `restored'"
    local failed_tests "`failed_tests' 3"
}

// Test Case 4: Variable Label Preservation
display _newline "=== TEST CASE 4: Variable Label Preservation ==="
local ++total_tests
use "test_restored.dta", clear
local vlab : var label mpg
if "`vlab'" == "`expected_mpg_label'" {
    display as result "Test 4 completed successfully"
    display "  Variable label: `vlab'"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 failed: variable label mismatch"
    display as error "  Expected: `expected_mpg_label'"
    display as error "  Got:      `vlab'"
    local failed_tests "`failed_tests' 4"
}

// Test Case 5: Value Label Preservation
display _newline "=== TEST CASE 5: Value Label Preservation ==="
local ++total_tests
use "test_restored.dta", clear
local l0 : label origin_lab 0
local l1 : label origin_lab 1
if "`l0'" == "`expected_l0'" & "`l1'" == "`expected_l1'" {
    display as result "Test 5 completed successfully"
    display "  Label 0: `l0'"
    display "  Label 1: `l1'"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed: value label mismatch"
    display as error "  Expected 0: `expected_l0', Got: `l0'"
    display as error "  Expected 1: `expected_l1', Got: `l1'"
    local failed_tests "`failed_tests' 5"
}

// Test Case 6: Expanded Dataset Integrity
display _newline "=== TEST CASE 6: Expanded Dataset Integrity ==="
local ++total_tests
use "test_restored.dta", clear
if c(N) == 7400 {
    display as result "Test 6 completed successfully"
    display "  Observations: `c(N)'"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: observation count mismatch"
    display as error "  Expected: 7400, Got: `c(N)'"
    local failed_tests "`failed_tests' 6"
}

// Test Case 7: Notes Preservation
display _newline "=== TEST CASE 7: Notes Preservation ==="
local ++total_tests
use "test_restored.dta", clear
local dta_note_count : char _dta[note0]
local make_note_count : char make[note0]
local t7_err 0

// Check dataset notes match expected count
if `"`dta_note_count'"' != "`expected_dta_note_count'" {
    display as error "Dataset note count mismatch: `dta_note_count' (expected: `expected_dta_note_count')"
    local ++t7_err
}

// Check variable notes match expected count  
if `"`make_note_count'"' != "`expected_make_note_count'" {
    display as error "Variable note count mismatch: `make_note_count' (expected: `expected_make_note_count')"
    local ++t7_err
}

// Compare each note by capturing source notes first, then comparing with restored
use "`original_dta'", clear
notes
forvalues i = 1/`expected_dta_note_count' {
    local src_dta_note_`i' : char _dta[note`i']
    display `"src_dta_note_`i': `src_dta_note_`i''"'
}

use "test_restored.dta", clear
notes
forvalues i = 1/`expected_dta_note_count' {
    local rst_note : char _dta[note`i']
    display `"DEBUG dta`i' char: [`rst_note']"'
    if strtrim(`"`src_dta_note_`i''"') != strtrim(`"`rst_note'"') {
        display as error "Dataset note `i' mismatch"
        display as error `"  Expected: [`src_dta_note_`i'']"'
        display as error "  Got:      [`rst_note']"
        local ++t7_err
    }
}

if `t7_err' == 0 {
    display as result "Test 7 completed successfully"
    display "  Dataset notes: `dta_note_count' preserved"
    display "  Variable notes: `make_note_count' preserved"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: `t7_err' note preservation error(s)"
    local failed_tests "`failed_tests' 7"
}

// Test Case 8: Streaming with Very Small Chunksize (Boundary Test)
display _newline "=== TEST CASE 8: Streaming with Very Small Chunksize ==="
local ++total_tests
use "`original_dta'", clear
dtparquet export "test_tiny_chunk.parquet" using "`original_dta'", replace chunksize(10)
if _rc == 0 {
    dtparquet import "test_tiny_restored.dta" using "test_tiny_chunk.parquet", replace chunksize(7)
    if _rc == 0 {
        use "test_tiny_restored.dta", clear
        datasignature
        local tiny_restored = r(datasignature)
        if "`baseline'" == "`tiny_restored'" {
            display as result "Test 8 completed successfully"
            display "  Chunksize export: 10, import: 7"
            local passed_tests "`passed_tests' 8"
        }
        else {
            display as error "Test 8 failed: DataSignature mismatch with tiny chunks"
            local failed_tests "`failed_tests' 8"
        }
    }
    else {
        display as error "Test 8 failed: import with tiny chunksize failed, rc=" _rc
        local failed_tests "`failed_tests' 8"
    }
}
else {
    display as error "Test 8 failed: export with tiny chunksize failed, rc=" _rc
    local failed_tests "`failed_tests' 8"
}

// Test Case 9: Streaming with Large Chunksize (Above N)
display _newline "=== TEST CASE 9: Streaming with Large Chunksize ==="
local ++total_tests
use "`original_dta'", clear
dtparquet export "test_large_chunk.parquet" using "`original_dta'", replace chunksize(10000)
if _rc == 0 {
    dtparquet import "test_large_restored.dta" using "test_large_chunk.parquet", replace chunksize(10000)
    if _rc == 0 {
        use "test_large_restored.dta", clear
        if c(N) == 7400 & c(k) > 0 {
            display as result "Test 9 completed successfully"
            display "  Chunksize: 10000 (larger than N)"
            local passed_tests "`passed_tests' 9"
        }
        else {
            display as error "Test 9 failed: data dimensions incorrect"
            local failed_tests "`failed_tests' 9"
        }
    }
    else {
        display as error "Test 9 failed: import with large chunksize failed, rc=" _rc
        local failed_tests "`failed_tests' 9"
    }
}
else {
    display as error "Test 9 failed: export with large chunksize failed, rc=" _rc
    local failed_tests "`failed_tests' 9"
}

// Cleanup
capture erase "test_stream.parquet"
capture erase "test_restored.dta"
capture erase "test_tiny_chunk.parquet"
capture erase "test_tiny_restored.dta"
capture erase "test_large_chunk.parquet"
capture erase "test_large_restored.dta"

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
    display as result "All tests passed!"
    log close
    exit 0
}
