* dtparquet_test5.do
* Verification for Phase 5: Optimized SFI Streaming (Row-Major)
* Date: Jan 13, 2026

version 16
clear all
macro drop _all

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

// Install local versions
local ado_plus = c(sysdir_plus)
copy ado/dtparquet.ado "`ado_plus'd/dtparquet.ado", replace
copy ado/dtparquet.py "`ado_plus'd/dtparquet.py", replace
discard

display _newline(2) "=========================================="
display "Starting dtparquet Phase 5 Test Suite"
display "==========================================" _newline

// Test Case 1: Wide Data (Row-Major check)
display "--- Test 1: Wide Data ---"
clear
set obs 100
gen id = _n
forvalues i = 1/200 {
    gen var`i' = _n * `i'
}
dtparquet save "test_wide.parquet", replace chunksize(20)
clear
dtparquet use using "test_wide.parquet"
assert c(k) == 201
assert c(N) == 100
assert var200 == id * 200
display "Test 1 Passed"

// Test Case 2: Chunk Boundary Test
display "--- Test 2: Chunk Boundary (N not multiple of chunksize) ---"
clear
set obs 55
gen id = _n
dtparquet save "test_boundary.parquet", replace chunksize(10)
clear
dtparquet use using "test_boundary.parquet"
assert c(N) == 55
display "Test 2 Passed"

// Test Case 3: UTF-8 Symmetric Handling
display "--- Test 3: UTF-8 Special Characters ---"
clear
set obs 3
gen strL s = ""
replace s = "Standard text" in 1
replace s = "Unicode: € £ ¥ ©" in 2
replace s = "Emoji: 🚀 📊 📈" in 3
dtparquet save "test_utf8.parquet", replace
clear
dtparquet use using "test_utf8.parquet"
assert s == "Standard text" in 1
assert s == "Unicode: € £ ¥ ©" in 2
assert s == "Emoji: 🚀 📊 📈" in 3
display "Test 3 Passed"

// Test Case 4: Manual Chunksize Override
display "--- Test 4: Chunksize Override ---"
clear
set obs 10
gen x = _n
dtparquet save "test_chunk.parquet", replace chunksize(1)
clear
dtparquet use using "test_chunk.parquet", chunksize(1)
assert x == _n
display "Test 4 Passed"

// Test Case 5: Data Signature Fidelity
display "--- Test 5: Data Signature Fidelity ---"
clear
set obs 100
gen id = _n
gen byte b = mod(_n, 10)
gen int i = _n * 10
gen long l = _n * 100
gen float f = _n * 1.1
gen double d = _n * 1.123456789
gen str10 s = "row " + string(_n)
gen strL sl = "large " + string(_n)
datasignature
local sig_orig = r(datasignature)

dtparquet save "test_sig.parquet", replace chunksize(15)
clear
dtparquet use using "test_sig.parquet"
datasignature
assert r(datasignature) == "`sig_orig'"
display "Test 5 Passed"

// Cleanup
capture erase "test_wide.parquet"
capture erase "test_boundary.parquet"
capture erase "test_utf8.parquet"
capture erase "test_chunk.parquet"
capture erase "test_sig.parquet"

display _newline "All Phase 5 tests passed!"
exit
