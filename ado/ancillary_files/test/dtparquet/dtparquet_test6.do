* dtparquet_test6.do
* Test suite for command and option abbreviations
* Date: Jan 14, 2026

version 16
clear frames
capture log close
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using "ado/ancillary_files/test/log/dtparquet_test6.log", replace

// Load programs from ado directory
discard
local ado_plus = c(sysdir_plus)
capture mkdir "`ado_plus'd"
copy "ado/dtparquet.ado" "`ado_plus'd/dtparquet.ado", replace
copy "ado/dtparquet.py" "`ado_plus'd/dtparquet.py", replace

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Abbreviation Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Ensure Python is configured
python query
if r(initialized) != 1 {
    set python_exec "C:/Users/hafiz/AppData/Local/Python/pythoncore-3.14-64/python.exe"
}

// Setup common data
set obs 10
generate id = _n
label variable id "Identifier"
save "test_base.dta", replace

// Test Case 1: 'dtparquet sa' (save) and 're' (replace)
display _newline "=== TEST CASE 1: Subcommand 'sa' and option 're' ==="
local ++total_tests
dtparquet sa "test_abbr.parquet", replace
if _rc == 0 {
    dtparquet sa "test_abbr.parquet", re
    if _rc == 0 {
        display as result "Test 1 completed successfully"
        local passed_tests "`passed_tests' 1"
    }
    else {
        display as error "Test 1 failed: 're' abbreviation not recognized"
        local failed_tests "`failed_tests' 1"
    }
}
else {
    display as error "Test 1 failed: 'sa' abbreviation or 'replace' failed"
    local failed_tests "`failed_tests' 1"
}

// Test Case 2: 'dtparquet u' (use) and 'cle' (clear)
display _newline "=== TEST CASE 2: Subcommand 'u' and option 'cle' ==="
local ++total_tests
dtparquet u "test_abbr.parquet", cle
if _rc == 0 & c(N) == 10 {
    display as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    display as error "Test 2 failed: rc=" _rc " N=" c(N)
    local failed_tests "`failed_tests' 2"
}

// Test Case 3: 'dtparquet exp' (export)
display _newline "=== TEST CASE 3: Subcommand 'exp' (export) ==="
local ++total_tests
dtparquet exp "test_exp.parquet" using "test_base.dta", re
if _rc == 0 {
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: rc=" _rc
    local failed_tests "`failed_tests' 3"
}

// Test Case 4: 'dtparquet imp' (import)
display _newline "=== TEST CASE 4: Subcommand 'imp' (import) ==="
local ++total_tests
dtparquet imp "test_imp.dta" using "test_exp.parquet", re
if _rc == 0 {
    capture confirm file "test_imp.dta"
    if _rc == 0 {
        display as result "Test 4 completed successfully"
        local passed_tests "`passed_tests' 4"
    }
    else {
        display as error "Test 4 failed: output file not found"
        local failed_tests "`failed_tests' 4"
    }
}
else {
    display as error "Test 4 failed: rc=" _rc
    local failed_tests "`failed_tests' 4"
}

// Test Case 5: 'nol' (nolabel) and 'ch' (chunksize)
display _newline "=== TEST CASE 5: Options 'nol' and 'ch' ==="
local ++total_tests
dtparquet sa "test_opts.parquet", re nol ch(1000)
if _rc == 0 {
    dtparquet u "test_opts.parquet", cle nol
    local vlab : var label id
    if "`vlab'" == "" {
        display as result "Test 5 completed successfully"
        local passed_tests "`passed_tests' 5"
    }
    else {
        display as error "Test 5 failed: label not removed with 'nol'"
        local failed_tests "`failed_tests' 5"
    }
}
else {
    display as error "Test 5 failed: rc=" _rc
    local failed_tests "`failed_tests' 5"
}

// Test Case 6: 'dtparquet u [varlist] using [file]'
display _newline "=== TEST CASE 6: Use with Varlist and Using Keyword ==="
local ++total_tests
dtparquet u id using "test_abbr.parquet", cle
if _rc == 0 & c(k) == 1 {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: rc=" _rc " k=" c(k)
    local failed_tests "`failed_tests' 6"
}

// Test Case 7: 'all' (allstring)
display _newline "=== TEST CASE 7: Option 'all' (allstring) ==="
local ++total_tests
dtparquet u "test_abbr.parquet", cle all
if _rc == 0 {
    display as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: rc=" _rc
    local failed_tests "`failed_tests' 7"
}

// Test Case 8: String columns with null values (None in Python)
display _newline "=== TEST CASE 8: String columns with null values ==="
local ++total_tests
python:
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
try:
    # Create a dataframe with various types including null type and long strings
    schema = pa.schema([
        ("id", pa.int64()),
        ("str_col", pa.string()),
        ("null_col", pa.null()),
        ("long_str", pa.string()),
        ("bool_col", pa.bool_())
    ])
    
    data = [
        pa.array([1, 2, 3]),
        pa.array(["a", None, "c"]),
        pa.array([None, None, None], type=pa.null()),
        pa.array(["Short", "A" * 3000, None]),
        pa.array([True, False, None])
    ]
    table = pa.Table.from_arrays(data, schema=schema)
    pq.write_table(table, 'test_complex.parquet')
    print("Created test_complex.parquet")
except Exception as e:
    print(f"Error creating test parquet: {e}")
end

dtparquet u "test_complex.parquet", cle
display _rc
if _rc == 0 {
    describe
    list
    if long_str[2] == "A" * 3000 & null_col[1] == "" {
        display as result "Test 8/9 complex completed successfully"
        local passed_tests "`passed_tests' 8"
    }
    else {
        display as error "Test 8/9 failed: data mismatch"
        local failed_tests "`failed_tests' 8"
    }
}
else {
    display as error "Test 8/9 failed: rc=" _rc
    local failed_tests "`failed_tests' 8"
}

// Test Case 9: Real-world complex file (BPOM)
display _newline "=== TEST CASE 9: BPOM real-world file ==="
local ++total_tests
dtparquet u "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", cle
display _rc

if _rc == 0 {
    count
    if r(N) > 0 {
        display as result "Test 9 completed successfully: Loaded " r(N) " observations"
        local passed_tests "`passed_tests' 9"
    }
    else {
        display as error "Test 9 failed: No observations loaded"
        local failed_tests "`failed_tests' 9"
    }
}
else {
    display as error "Test 9 failed: rc=" _rc
    local failed_tests "`failed_tests' 9"
}

// Cleanup
capture erase "test_base.dta"
capture erase "test_abbr.parquet"
capture erase "test_exp.parquet"
capture erase "test_imp.dta"
capture erase "test_opts.parquet"
capture erase "test_null_str.parquet"
capture erase "test_complex.parquet"

// Summary
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
