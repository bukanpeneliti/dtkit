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
copy ado/dtparquet.py "`ado_plus'd/dtparquet.py", replace

local total_tests 0
local passed_tests 0

display _newline "=========================================="
display "Starting dtparquet Phase 3 Test Suite"
display "=========================================="

// Test 1: Compression is NONE
display _newline "=== TEST 1: Compression is NONE ==="
local ++total_tests
clear
set obs 100
gen x = _n
dtparquet save "test_compression.parquet", replace

python:
import pyarrow.parquet as pq
parquet_file = pq.ParquetFile("test_compression.parquet")
# Check compression of the first row group, first column
compression = parquet_file.metadata.row_group(0).column(0).compression
print(f"DEBUG: Compression is {compression}")
# Explicitly close or delete to release handle
del parquet_file
if compression != "UNCOMPRESSED":
    raise ValueError(f"Expected UNCOMPRESSED, got {compression}")
end

if _rc == 0 {
    display as result "Test 1 passed: File is uncompressed."
    local ++passed_tests
}
else {
    display as error "Test 1 failed: File is compressed or metadata check failed."
}

// Test 2: Version Gate (min_reader_version)
display _newline "=== TEST 2: Version Gate ==="
local ++total_tests

// Create a file and manually inject a high min_reader_version
python:
import pyarrow.parquet as pq
import json
import pyarrow as pa

# Read the file we just saved
table = pq.read_table("test_compression.parquet")
meta = table.schema.metadata
dtmeta_json = meta[b"dtparquet.dtmeta"].decode()
dtmeta = json.loads(dtmeta_json)

# Inject high version
dtmeta["min_reader_version"] = 999
new_meta = meta.copy()
new_meta[b"dtparquet.dtmeta"] = json.dumps(dtmeta).encode()
table = table.replace_schema_metadata(new_meta)

pq.write_table(table, "test_version_gate.parquet")
# Release handles
del table
end

capture dtparquet use using "test_version_gate.parquet", clear
if _rc != 0 {
    display as result "Test 2 passed: Successfully blocked high-version file."
    local ++passed_tests
}
else {
    display as error "Test 2 failed: Should have blocked high-version file but did not."
}

// Cleanup
capture erase "test_compression.parquet"
capture erase "test_version_gate.parquet"

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
