* dtparquet_test5.do
* Verification for Phase 5: Optimized SFI Streaming (Row-Major) and Phase 6: Advanced Type Mapping
* Date: Jan 13, 2026

version 16
clear all
macro drop _all

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using ado/ancillary_files/test/log/dtparquet_test5.log, replace

// Install local versions
discard
local ado_plus = c(sysdir_plus)
copy "ado/dtparquet.ado" "`ado_plus'd/dtparquet.ado", replace
copy "ado/dtparquet.py"  "`ado_plus'd/dtparquet.py", replace

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Phase 5 & 6 Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Wide Data (Row-Major check)
display _newline "=== TEST CASE 1: Wide Data ==="
local ++total_tests
clear
set obs 100
gen id = _n
forvalues i = 1/200 {
    gen var`i' = _n * `i'
}
capture dtparquet save "test_wide.parquet", replace chunksize(20)
if _rc != 0 {
    display as error "Test 1 failed: save error " _rc
    local failed_tests "`failed_tests' 1"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_wide.parquet"
        assert c(k) == 201
        assert c(N) == 100
        assert var200 == id * 200
    }
    if _rc == 0 {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: assertion error"
        local failed_tests "`failed_tests' 1"
    }
}

// Test Case 2: Chunk Boundary Test
display _newline "=== TEST CASE 2: Chunk Boundary (N not multiple of chunksize) ==="
local ++total_tests
clear
set obs 55
gen id = _n
capture dtparquet save "test_boundary.parquet", replace chunksize(10)
if _rc != 0 {
    display as error "Test 2 failed: save error " _rc
    local failed_tests "`failed_tests' 2"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_boundary.parquet"
        assert c(N) == 55
    }
    if _rc == 0 {
        display as result "Test 2 completed successfully"
        local passed_tests "`passed_tests' 2"
    }
    else {
        display as error "Test 2 failed: assertion error"
        local failed_tests "`failed_tests' 2"
    }
}

// Test Case 3: UTF-8 Symmetric Handling
display _newline "=== TEST CASE 3: UTF-8 Special Characters ==="
local ++total_tests
clear
set obs 3
gen strL s = ""
replace s = "Standard text" in 1
replace s = "Unicode: € £ ¥ ©" in 2
replace s = "Emoji: 🚀 📊 📈" in 3
capture dtparquet save "test_utf8.parquet", replace
if _rc != 0 {
    display as error "Test 3 failed: save error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_utf8.parquet"
        assert s == "Standard text" in 1
        assert s == "Unicode: € £ ¥ ©" in 2
        assert s == "Emoji: 🚀 📊 📈" in 3
    }
    if _rc == 0 {
        display as result "Test 3 completed successfully"
        local passed_tests "`passed_tests' 3"
    }
    else {
        display as error "Test 3 failed: assertion error"
        local failed_tests "`failed_tests' 3"
    }
}

// Test Case 4: Manual Chunksize Override
display _newline "=== TEST CASE 4: Chunksize Override ==="
local ++total_tests
clear
set obs 10
gen x = _n
capture dtparquet save "test_chunk.parquet", replace chunksize(1)
if _rc != 0 {
    display as error "Test 4 failed: save error " _rc
    local failed_tests "`failed_tests' 4"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_chunk.parquet", chunksize(1)
        assert x == _n
    }
    if _rc == 0 {
        display as result "Test 4 completed successfully"
        local passed_tests "`passed_tests' 4"
    }
    else {
        display as error "Test 4 failed: assertion error"
        local failed_tests "`failed_tests' 4"
    }
}

// Test Case 5: Data Signature Fidelity
display _newline "=== TEST CASE 5: Data Signature Fidelity ==="
local ++total_tests
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

capture dtparquet save "test_sig.parquet", replace chunksize(15)
if _rc != 0 {
    display as error "Test 5 failed: save error " _rc
    local failed_tests "`failed_tests' 5"
}
else {
    capture noisily {
        clear
        dtparquet use using "test_sig.parquet"
        datasignature
        assert r(datasignature) == "`sig_orig'"
    }
    if _rc == 0 {
        display as result "Test 5 completed successfully"
        local passed_tests "`passed_tests' 5"
    }
    else {
        display as error "Test 5 failed: datasignature mismatch"
        local failed_tests "`failed_tests' 5"
    }
}

// Test Case 6: Foreign Parquet File Generation
display _newline "=== TEST CASE 6: Foreign Parquet File Generation ==="
local ++total_tests

cap python: import pyarrow as pa
cap python: import pyarrow.parquet as pq
if _rc {
    display as error "Test 6 failed: pyarrow not available"
    local failed_tests "`failed_tests' 6"
}
else {
    python: data = {}
    python: categories = pa.array(["Apple", "Banana", "Cherry"])
    python: indices = pa.array([0, 1, 0, 2, 1, 0], type=pa.int8())
    python: data["category"] = pa.DictionaryArray.from_arrays(indices, categories)
    python: data["unix_date"] = pa.array([0, 365, 730, 1095, 1460, 1825], type=pa.date32())
    python: data["unix_timestamp"] = pa.array([0, 86400000, 172800000, 259200000, 345600000, 432000000], type=pa.timestamp('ms'))
    python: data["huge_int64"] = pa.array([9223372036854775807, 0, -1, 100, 200, 300], type=pa.int64())
    python: data["binary_blob"] = pa.array([b'\x00\x01\x02\xff', b'test', b'\x80\x81', b'', b'abc', b'def'], type=pa.binary())
    python: pq.write_table(pa.table(data), "test_foreign.parquet")
    
    capture confirm file "test_foreign.parquet"
    if _rc == 0 {
        display as result "Test 6 completed successfully"
        local passed_tests "`passed_tests' 6"
    }
    else {
        display as error "Test 6 failed: File not created"
        local failed_tests "`failed_tests' 6"
    }
}

// Test Case 7: Dictionary Import
display _newline "=== TEST CASE 7: Dictionary Import ==="
local ++total_tests
capture noisily {
    clear
    dtparquet use category using "test_foreign.parquet", clear
    local t : type category
    assert inlist("`t'", "byte", "int", "long")
    local cat_label : value label category
    assert "`cat_label'" != ""
    assert category == 0 in 1
    assert category == 1 in 2
    datasignature
    local sig = r(datasignature)
    display "Signature: `sig'"
}
if _rc == 0 {
    display as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: dictionary import error"
    local failed_tests "`failed_tests' 7"
}

// Test Case 8: Foreign Date/Time Epoch Conversion
display _newline "=== TEST CASE 8: Foreign Date/Time (Epoch Adjustment) ==="
local ++total_tests
capture noisily {
    clear
    dtparquet use unix_date unix_timestamp using "test_foreign.parquet", clear
    assert unix_date == td(01jan1970) in 1
    assert unix_date == td(01jan1971) in 2
    assert unix_timestamp == clock("01jan1970 00:00:00", "DMYhms") in 1
    datasignature
    local sig = r(datasignature)
    display "Signature: `sig'"
}
if _rc == 0 {
    display as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: epoch conversion error"
    local failed_tests "`failed_tests' 8"
}

// Test Case 9a: Int64 Precision (Default = Double)
display _newline "=== TEST CASE 9a: Int64 Precision (Default = Double) ==="
local ++total_tests
capture noisily {
    clear
    dtparquet use huge_int64 using "test_foreign.parquet", clear
    local t : type huge_int64
    assert "`t'" == "double"
    datasignature
    local sig = r(datasignature)
    display "Signature: `sig'"
}
if _rc == 0 {
    display as result "Test 9a completed successfully"
    local passed_tests "`passed_tests' 9a"
}
else {
    display as error "Test 9a failed: type mapping error"
    local failed_tests "`failed_tests' 9a"
}

display _newline "=== TEST CASE 9b: Int64 Precision (allstring option) ==="
local ++total_tests
capture noisily {
    clear
    dtparquet use huge_int64 using "test_foreign.parquet", allstring clear
    local t : type huge_int64
    assert "`t'" == "strL"
    assert huge_int64 == "9223372036854775807" in 1
    datasignature
    local sig = r(datasignature)
    display "Signature: `sig'"
}
if _rc == 0 {
    display as result "Test 9b completed successfully"
    local passed_tests "`passed_tests' 9b"
}
else {
    display as error "Test 9b failed: allstring error"
    local failed_tests "`failed_tests' 9b"
}

// Test Case 10: Binary Blob Handling
display _newline "=== TEST CASE 10: Binary Blob Safety ==="
local ++total_tests
capture noisily {
    clear
    dtparquet use binary_blob using "test_foreign.parquet", clear
    local t : type binary_blob
    assert "`t'" == "strL"
    datasignature
    local sig = r(datasignature)
    display "Signature: `sig'"
}
if _rc == 0 {
    display as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    display as error "Test 10 failed: binary import error"
    local failed_tests "`failed_tests' 10"
}

// Test Case 11: Native Round-Trip (No Epoch Offset)
display _newline "=== TEST CASE 11: Native Round-Trip (No Epoch Offset) ==="
local ++total_tests
capture noisily {
    clear
    set obs 3
    gen id = _n
    gen date = td(01jan2020) + (_n - 1)
    format date %td
    gen double ts = clock("01jan2020 00:00:00", "DMYhms") + (_n - 1) * 86400000
    format ts %tc
    datasignature
    local sig_orig = r(datasignature)
    
    dtparquet save "test_native.parquet", replace
    clear
    dtparquet use using "test_native.parquet"
    datasignature
    assert r(datasignature) == "`sig_orig'"
}
if _rc == 0 {
    display as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}
else {
    display as error "Test 11 failed: native round-trip error"
    local failed_tests "`failed_tests' 11"
}

// Cleanup
capture erase "test_wide.parquet"
capture erase "test_boundary.parquet"
capture erase "test_utf8.parquet"
capture erase "test_chunk.parquet"
capture erase "test_sig.parquet"
capture erase "test_foreign.parquet"
capture erase "test_native.parquet"

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
