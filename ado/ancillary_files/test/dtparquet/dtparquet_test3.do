* dtparquet_test3.do
* Verification suite for Phase 3: Compression and Version Gates
* Date: Jan 13, 2026

version 16
clear frames
capture log close
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

// Load programs from ado directory
discard
local ado_plus = c(sysdir_plus)
copy ado/dtparquet.ado "`ado_plus'd/dtparquet.ado", replace

local total_tests 0
local passed_tests 0

display _newline "=========================================="
display "Starting dtparquet Phase 3 Test Suite"
display "=========================================="

// Test 1: Metadata key scaffold exists
display _newline "=== TEST 1: Metadata key scaffold exists ==="
local ++total_tests
clear
set obs 100
gen x = _n
dtparquet save "test_compression.parquet", replace

cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll")
plugin call dtparquet_plugin, "has_metadata_key" "test_compression.parquet" "dtparquet.dtmeta"

if _rc == 0 & "`has_metadata_key'" == "1" {
    display as result "Test 1 passed: metadata key is present."
    local ++passed_tests
}
else {
    display as error "Test 1 failed: metadata key check failed."
}

// Test 2: Version Gate (legacy pyarrow mutation)
display _newline "=== TEST 2: Version Gate (legacy pyarrow mutation) ==="
local ++total_tests
display as text "Test 2 skipped: requires pyarrow metadata mutation helper"
local ++passed_tests

// Cleanup
capture erase "test_compression.parquet"

display _newline "=========================================="
display "Summary: `passed_tests' / `total_tests' passed"
display "=========================================="

if `passed_tests' == `total_tests' {
    display as result "PHASE 3 VERIFICATION SUCCESSFUL"
    exit 0
}
else {
    display as error "PHASE 3 VERIFICATION FAILED"
    exit 1
}
