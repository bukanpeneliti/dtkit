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
run "ado/dtparquet.ado"
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

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Abbreviation Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

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

// Test Case 8: String columns with empty/long strings
display _newline "=== TEST CASE 8: String columns with null values ==="
local ++total_tests
clear
set obs 3
gen long id = _n
gen str20 str_col = ""
replace str_col = "a" in 1
replace str_col = "" in 2
replace str_col = "c" in 3
gen str200 long_str = ""
replace long_str = "Short" in 1
replace long_str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" in 2
replace long_str = "" in 3
dtparquet save "test_complex.parquet", replace

dtparquet u "test_complex.parquet", cle
display _rc
if _rc == 0 {
    describe
    list
    if substr(long_str[2],1,1) == "A" & length(long_str[2]) > 90 & str_col[2] == "" {
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
    capture erase "dtparquet_test6.log"
    exit 0
}
