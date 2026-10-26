* dtparquet_test1.do
* Consolidated CORE suite for dtparquet.
* Covers: command availability, plugin version/API guard, save/use roundtrip (all
* types + datasignature), export/import roundtrip, replace protection, frame
* isolation, tmp-file behavior, export/import parser errors, varlist subsetting,
* if/in, nolabel, extension handling, missing-file error, abbreviations, describe
* variants, foreign detailed describe, metadata-key scaffold.
* Consolidates: old test1 (Phase 1), old test2 (export/import), old test3
* (compression/version gates), old test6 (abbreviations), parser checks from
* old test7 T12.
* Deduplicated away: old test2 T3 weak row-count spot check (superseded by the
* real datasignature compare in C4), old test2 T4 weak metadata spot check
* (covered strongly in test2 C1), old test3 T2 skipped pyarrow gate, redundant
* second copies of varlist/if/nolabel (strongest form kept once).
* Run from the checkout root.

version 16
clear frames
capture log close _all
capture confirm file "ado/dtparquet.ado"
if _rc != 0 {
    if c(hostname) == "NUXS" {
        cd d:/OneDrive/MyWork/00personal/stata/dtkit
    }
    else {
        cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
    }
}

log using ado/ancillary_files/test/log/dtparquet_test1.log, replace

// Load programs from ado directory
discard
capture program drop dtparquet
run "ado/dtparquet.ado"
capture program drop dtmeta
capture program drop _makevars
capture program drop _makevarnotes
capture program drop _makevallab
capture program drop _makedtainfo
capture program drop _isempty
capture program drop _labelframes
capture program drop _toexcel
capture program drop _argload
capture program drop _makereport
run "ado/dtmeta.ado"
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/dtparquet.dll"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

display _newline(2) "=========================================="
display "Starting dtparquet CORE Test Suite (consolidated)"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: dtparquet command availability
display _newline "=== TEST CASE 1: dtparquet command availability ==="
timer clear 1
timer on 1
local ++total_tests
capture which dtparquet
if _rc == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: dtparquet command not available, rc=" _rc
    local failed_tests "`failed_tests' 1"
}
timer off 1
timer list 1
display as text "Test 1 finished in" as result %6.2f r(t1) "s"

// Test Case 2: Plugin version and API guard checks (old test1 T1b)
display _newline "=== TEST CASE 2: Plugin Version/API Guard ==="
timer clear 2
timer on 2
local ++total_tests

local t2_err 0
capture plugin call dtparquet_plugin, "version"
if _rc {
    display as error "Test 2 failed: plugin version call failed, rc=" _rc
    local ++t2_err
}
else if `"`dtparquet_plugin_version'"' == "" {
    display as error "Test 2 failed: plugin version macro is empty"
    local ++t2_err
}

capture noisily dtparquet__verify_plugin_version
if _rc {
    display as error "Test 2 failed: expected compatible API, rc=" _rc
    local ++t2_err
}

capture program drop dtparquet__verify_plugin_version
program dtparquet__verify_plugin_version
    local expected_api "999.0"
    capture plugin call dtparquet_plugin, "version"
    if _rc exit _rc

    local plugin_version `"`dtparquet_plugin_version'"'
    local plugin_api ""
    gettoken pv_major pv_rest : plugin_version, parse(".")
    if `"`pv_rest'"' != "" {
        local pv_rest = substr(`"`pv_rest'"', 2, .)
        gettoken pv_minor pv_patch : pv_rest, parse(".")
        if `"`pv_major'"' != "" & `"`pv_minor'"' != "" {
            local plugin_api `"`pv_major'.`pv_minor'"'
        }
    }

    if `"`plugin_api'"' != `"`expected_api'"' {
        exit 459
    }
end

capture noisily dtparquet__verify_plugin_version
if _rc != 459 {
    display as error "Test 2 failed: expected mismatch rc=459, got rc=" _rc
    local ++t2_err
}

capture program drop dtparquet
run "ado/dtparquet.ado"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

if `t2_err' == 0 {
    display as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    local failed_tests "`failed_tests' 2"
}
timer off 2
timer list 2
display as text "Test 2 finished in" as result %6.2f r(t2) "s"

// Test Case 3: Basic Save and Use roundtrip with all types (old test1 T2)
display _newline "=== TEST CASE 3: Basic Save and Use (All Data Types) ==="
timer clear 3
timer on 3
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate float v_float = _n * 1.1
generate double v_double = _n * 1.123456789
generate str10 v_str = "row " + string(_n)
generate str60 v_strl = "large string for row " + string(_n)
generate v_date = td(01jan2020) + _n
format v_date %td

save "t1c3_source.dta", replace

dtparquet save "t1c3.parquet", replace
if _rc {
    display as error "Test 3 Save failed with error " _rc
    local failed_tests "`failed_tests' 3"
}
else {
    clear
    dtparquet use using "t1c3.parquet"
    if _rc {
        display as error "Test 3 Use failed with error " _rc
        local failed_tests "`failed_tests' 3"
    }
    else {
        local t3_err 0
        capture assert v_byte == _n
        if _rc local ++t3_err
        capture assert v_str == "row " + string(_n)
        if _rc local ++t3_err
        capture assert v_date == td(01jan2020) + _n
        if _rc local ++t3_err

        save "t1c3_roundtrip.dta", replace

        use "t1c3_source.dta", clear
        datasignature
        local sig_before = r(datasignature)

        use "t1c3_roundtrip.dta", clear
        datasignature
        local sig_after = r(datasignature)

        if "`sig_before'" != "`sig_after'" {
            display as error "Test 3 failed: datasignature mismatch"
            display as error "  Before: `sig_before'"
            display as error "  After:  `sig_after'"
            local ++t3_err
        }

        if `t3_err' == 0 {
            display as result "Test 3 completed successfully"
            local passed_tests "`passed_tests' 3"
        }
        else {
            display as error "Test 3 verification failed"
            local failed_tests "`failed_tests' 3"
        }
    }
}
timer off 3
timer list 3
display as text "Test 3 finished in" as result %6.2f r(t3) "s"

// Test Case 4: Export/Import roundtrip with datasignature (old test2 T1-T3 merged)
display _newline "=== TEST CASE 4: Export/Import Roundtrip ==="
timer clear 4
timer on 4
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate float v_float = _n * 1.1
generate double v_double = _n * 1.123456789
generate str10 v_str = "row " + string(_n)
generate v_date = td(01jan2020) + _n
format v_date %td
save "t1c4_source.dta", replace

local t4_err 0
dtparquet export "t1c4.parquet" using "t1c4_source.dta", replace
if _rc {
    display as error "Test 4 Export failed with error " _rc
    local ++t4_err
}
else {
    capture confirm file "t1c4.parquet"
    if _rc {
        display as error "Test 4 failed: parquet file not created"
        local ++t4_err
    }
    else {
        dtparquet import "t1c4_import.dta" using "t1c4.parquet", replace
        if _rc {
            display as error "Test 4 Import failed with error " _rc
            local ++t4_err
        }
        else {
            use "t1c4_import.dta", clear
            capture assert c(N) == 10
            if _rc local ++t4_err
            capture assert c(k) == 7
            if _rc local ++t4_err
            capture assert v_byte[1] == 1
            if _rc local ++t4_err

            use "t1c4_source.dta", clear
            datasignature
            local sig4_before = r(datasignature)
            use "t1c4_import.dta", clear
            datasignature
            if r(datasignature) != "`sig4_before'" {
                display as error "Test 4 failed: export/import datasignature mismatch"
                local ++t4_err
            }
        }
    }
}
if `t4_err' == 0 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    local failed_tests "`failed_tests' 4"
}
timer off 4
timer list 4
display as text "Test 4 finished in" as result %6.2f r(t4) "s"

// Test Case 5: Replace protection + missing source (old test2 T6+T9)
display _newline "=== TEST CASE 5: Replace Protection / Missing Source ==="
timer clear 5
timer on 5
local ++total_tests
clear
set obs 1
generate x = 1
save "t1c5_source.dta", replace

local t5_err 0
dtparquet export "t1c5.parquet" using "t1c5_source.dta", replace
if _rc {
    display as error "Test 5 setup export failed, rc=" _rc
    local ++t5_err
}
capture dtparquet export "t1c5.parquet" using "t1c5_source.dta"
if _rc != 602 {
    display as error "Test 5 failed: expected error 602 (file already exists), got " _rc
    local ++t5_err
}
capture dtparquet export "t1c5_err.parquet" using "non_existent_source.dta", replace
if _rc != 601 {
    display as error "Test 5 failed: expected error 601 (file not found), got " _rc
    local ++t5_err
}
if `t5_err' == 0 {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    local failed_tests "`failed_tests' 5"
}
timer off 5
timer list 5
display as text "Test 5 finished in" as result %6.2f r(t5) "s"

// Test Case 6: Frame isolation (old test2 T7)
display _newline "=== TEST CASE 6: Frame Isolation ==="
timer clear 6
timer on 6
local ++total_tests
clear
set obs 5
generate original = _n
local orig_vars = c(k)
local orig_obs = c(N)

dtparquet export "t1c6_isolation.parquet" using "t1c5_source.dta", replace
if _rc == 0 & c(k) == `orig_vars' & c(N) == `orig_obs' {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: current dataset was modified"
    local failed_tests "`failed_tests' 6"
}
timer off 6
timer list 6
display as text "Test 6 finished in" as result %6.2f r(t6) "s"

// Test Case 7: Orphaned .tmp file behavior (old test2 T8)
display _newline "=== TEST CASE 7: Orphaned .tmp File Behavior ==="
timer clear 7
timer on 7
local ++total_tests
copy "t1c4.parquet" "t1c7.parquet.tmp", replace

clear
set obs 1
generate x = 1
dtparquet save "t1c7_dummy.parquet", replace

capture confirm file "t1c7.parquet.tmp"
if _rc == 0 {
    display as result "Test 7 completed successfully (.tmp file persisted)"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: .tmp file missing unexpectedly"
    local failed_tests "`failed_tests' 7"
}
timer off 7
timer list 7
display as text "Test 7 finished in" as result %6.2f r(t7) "s"

// Test Case 8: Export/import parser edge-case failures (old test7 T12)
display _newline "=== TEST CASE 8: Export/Import Parser Failures ==="
timer clear 8
timer on 8
local ++total_tests
clear
set obs 2
gen long id = _n
save "t1c8_source.dta", replace
dtparquet export "t1c8.parquet" using "t1c8_source.dta", replace
dtparquet import "t1c8_import.dta" using "t1c8.parquet", replace

local t8_err 0
capture dtparquet export "t1c8.parquet" "t1c8_source.dta"
if _rc != 198 local ++t8_err

capture dtparquet export using "t1c8_source.dta"
if _rc != 198 local ++t8_err

capture dtparquet export "t1c8.parquet" using
if _rc != 198 local ++t8_err

capture dtparquet import "t1c8_import.dta" "t1c8.parquet"
if _rc != 198 local ++t8_err

capture dtparquet import using "t1c8.parquet"
if _rc != 198 local ++t8_err

capture dtparquet import "t1c8_import.dta" using
if _rc != 198 local ++t8_err

if `t8_err' == 0 {
    display as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: export/import parser failures not stable"
    local failed_tests "`failed_tests' 8"
}
timer off 8
timer list 8
display as text "Test 8 finished in" as result %6.2f r(t8) "s"

// Test Case 9: Varlist subsetting (old test1 T4, covers old test6 T6)
display _newline "=== TEST CASE 9: Varlist Subsetting ==="
timer clear 9
timer on 9
local ++total_tests
clear
set obs 1
generate a = 1
generate b = 2
generate c = 3
dtparquet save "t1c9.parquet", replace

clear
dtparquet use a c using "t1c9.parquet"
if _rc == 0 & c(k) == 2 {
    display as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}
else {
    display as error "Test 9 failed: expected 2 variables, got " c(k)
    local failed_tests "`failed_tests' 9"
}
timer off 9
timer list 9
display as text "Test 9 finished in" as result %6.2f r(t9) "s"

// Test Case 10: IF/IN conditions (old test1 T6)
display _newline "=== TEST CASE 10: IF/IN conditions ==="
timer clear 10
timer on 10
local ++total_tests
clear
set obs 10
generate id = _n
dtparquet save "t1c10.parquet", replace

clear
dtparquet use using "t1c10.parquet" if id > 5 in 1/8
if _rc == 0 & c(N) == 3 {
    display as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    display as error "Test 10 failed: expected 3 observations, got " c(N)
    local failed_tests "`failed_tests' 10"
}
timer off 10
timer list 10
display as text "Test 10 finished in" as result %6.2f r(t10) "s"

// Test Case 11: nolabel option, save/use and export/import (old test1 T5 + test2 T5)
display _newline "=== TEST CASE 11: nolabel Option ==="
timer clear 11
timer on 11
local ++total_tests
local t11_err 0

clear
set obs 1
generate x = 1
label variable x "Label"
dtparquet save "t1c11.parquet", replace

clear
dtparquet use using "t1c11.parquet", nolabel
local vlab : var label x
if "`vlab'" != "" {
    display as error "Test 11 failed: save/use label should have been empty"
    local ++t11_err
}

clear
set obs 1
generate x = 1
label variable x "Label"
save "t1c11_source.dta", replace
foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
    capture frame drop `fr'
}
dtparquet export "t1c11_exp.parquet" using "t1c11_source.dta", replace nolabel
foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
    capture frame drop `fr'
}
clear
set obs 0
capture erase "t1c11_imp.dta"
dtparquet import "t1c11_imp.dta" using "t1c11_exp.parquet", replace nolabel
use "t1c11_imp.dta", clear
local vlab2 : var label x
if "`vlab2'" != "" {
    display as error "Test 11 failed: export/import label should have been empty, got '`vlab2''"
    local ++t11_err
}

if `t11_err' == 0 {
    display as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}
else {
    local failed_tests "`failed_tests' 11"
}
timer off 11
timer list 11
display as text "Test 11 finished in" as result %6.2f r(t11) "s"

// Test Case 12: Extension handling (old test1 T8)
display _newline "=== TEST CASE 12: Extension Handling ==="
timer clear 12
timer on 12
local ++total_tests
clear
set obs 1
gen x = 1

dtparquet save "t1c12_noext", replace
capture confirm file "t1c12_noext.parquet"
local rc12a = _rc

dtparquet save "t1c12_upper.PARQUET", replace
capture confirm file "t1c12_upper.parquet"
local rc12b = _rc

clear
dtparquet use using "t1c12_noext"
local rc12c = (_rc == 0 & c(N) == 1)

if `rc12a' == 0 & `rc12b' == 0 & `rc12c' == 1 {
    display as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    display as error "Test 12 failed: extension handling incorrect"
    display as error "  no-ext save rc: `rc12a', upper-ext save rc: `rc12b', no-ext use ok: `rc12c'"
    local failed_tests "`failed_tests' 12"
}
timer off 12
timer list 12
display as text "Test 12 finished in" as result %6.2f r(t12) "s"

// Test Case 13: Error handling, missing file (old test1 T7)
display _newline "=== TEST CASE 13: Error Handling (Missing File) ==="
timer clear 13
timer on 13
local ++total_tests
clear
capture dtparquet use using "non_existent_t1.parquet"
if _rc != 0 {
    display as result "Test 13 completed successfully (caught missing file)"
    local passed_tests "`passed_tests' 13"
}
else {
    display as error "Test 13 failed: did not catch missing file"
    local failed_tests "`failed_tests' 13"
}
timer off 13
timer list 13
display as text "Test 13 finished in" as result %6.2f r(t13) "s"

// Test Case 14: Abbreviated subcommands (old test6 T1-T4 merged)
display _newline "=== TEST CASE 14: Abbreviated Subcommands ==="
timer clear 14
timer on 14
local ++total_tests
clear
set obs 10
generate id = _n
label variable id "Identifier"
save "t1c14_base.dta", replace

local t14_err 0
dtparquet sa "t1c14.parquet", replace
if _rc != 0 {
    display as error "Test 14 failed: 'sa' abbreviation failed"
    local ++t14_err
}
else {
    dtparquet sa "t1c14.parquet", re
    if _rc != 0 {
        display as error "Test 14 failed: 're' abbreviation not recognized"
        local ++t14_err
    }
}
dtparquet u "t1c14.parquet", cle
if _rc != 0 | c(N) != 10 {
    display as error "Test 14 failed: 'u'/'cle' rc=" _rc " N=" c(N)
    local ++t14_err
}
dtparquet exp "t1c14_exp.parquet" using "t1c14_base.dta", re
if _rc != 0 {
    display as error "Test 14 failed: 'exp' rc=" _rc
    local ++t14_err
}
dtparquet imp "t1c14_imp.dta" using "t1c14_exp.parquet", re
if _rc != 0 {
    display as error "Test 14 failed: 'imp' rc=" _rc
    local ++t14_err
}
else {
    capture confirm file "t1c14_imp.dta"
    if _rc != 0 {
        display as error "Test 14 failed: import output file not found"
        local ++t14_err
    }
}
if `t14_err' == 0 {
    display as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
}
else {
    local failed_tests "`failed_tests' 14"
}
timer off 14
timer list 14
display as text "Test 14 finished in" as result %6.2f r(t14) "s"

// Test Case 15: Abbreviated options (old test6 T5-T7 merged)
display _newline "=== TEST CASE 15: Abbreviated Options ==="
timer clear 15
timer on 15
local ++total_tests
local t15_err 0
dtparquet sa "t1c15_opts.parquet", re nol ch(1000)
if _rc != 0 {
    display as error "Test 15 failed: save with 'nol'/'ch' rc=" _rc
    local ++t15_err
}
else {
    dtparquet u "t1c15_opts.parquet", cle nol
    local vlab15 : var label id
    if "`vlab15'" != "" {
        display as error "Test 15 failed: label not removed with 'nol'"
        local ++t15_err
    }
}
dtparquet u id using "t1c14.parquet", cle
if _rc != 0 | c(k) != 1 {
    display as error "Test 15 failed: abbreviated varlist use rc=" _rc " k=" c(k)
    local ++t15_err
}
dtparquet u "t1c14.parquet", cle all
if _rc != 0 {
    display as error "Test 15 failed: 'all' (allstring) rc=" _rc
    local ++t15_err
}
if `t15_err' == 0 {
    display as result "Test 15 completed successfully"
    local passed_tests "`passed_tests' 15"
}
else {
    local failed_tests "`failed_tests' 15"
}
timer off 15
timer list 15
display as text "Test 15 finished in" as result %6.2f r(t15) "s"

// Test Case 16: dtparquet describe variants (old test1 T9)
display _newline "=== TEST CASE 16: dtparquet describe ==="
timer clear 16
timer on 16
local ++total_tests
clear
set obs 10
generate byte v_byte = _n
generate int v_int = _n * 100
generate long v_long = _n * 10000
generate str10 v_str = "row " + string(_n)

dtparquet save "t1c16.parquet", replace

dtparquet describe using "t1c16.parquet"
local t16_err 0
if r(N) != 10 local ++t16_err
if r(k) != 4 local ++t16_err

clear
dtparquet describe using "t1c16.parquet", quietly
if r(N) != 10 local ++t16_err

clear
dtparquet describe using "t1c16.parquet", short

clear
dtparquet describe using "t1c16.parquet", simple

clear
dtparquet describe using "t1c16.parquet", detailed
if r(k) != 4 local ++t16_err

clear
dtparquet describe using "t1c16.parquet", fullnames
if r(k) != 4 local ++t16_err

clear
dtparquet describe using "t1c16.parquet", numbers
if r(k) != 4 local ++t16_err

clear
dtparquet describe using "t1c16.parquet", replace
count
if r(N) != 4 local ++t16_err
if c(k) != 5 local ++t16_err
capture confirm variable varname
if _rc != 0 local ++t16_err
capture confirm variable vartype
if _rc != 0 local ++t16_err
capture confirm variable format
if _rc != 0 local ++t16_err
capture confirm variable vallab
if _rc != 0 local ++t16_err
capture confirm variable varlab
if _rc != 0 local ++t16_err
if varname[1] != "v_byte" local ++t16_err
if vartype[4] != "string" local ++t16_err

if `t16_err' == 0 {
    display as result "Test 16 completed successfully"
    local passed_tests "`passed_tests' 16"
}
else {
    display as error "Test 16 failed: describe return scalars incorrect"
    local failed_tests "`failed_tests' 16"
}
timer off 16
timer list 16
display as text "Test 16 finished in" as result %6.2f r(t16) "s"

// Test Case 17: Foreign detailed describe smoke test (old test1 T10)
display _newline "=== TEST CASE 17: Foreign Detailed Describe ==="
timer clear 17
timer on 17
local ++total_tests
local foreign_large "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet"
local t17_err 0

capture plugin call dtparquet_plugin, "has_metadata_key" "`foreign_large'" "dtparquet.dtmeta"
if _rc != 0 local ++t17_err
else if "`has_metadata_key'" != "0" local ++t17_err

dtparquet describe using "`foreign_large'", detailed
if r(N) <= 0 local ++t17_err
if r(k) <= 0 local ++t17_err
display as text "Test 17 schema:" as result " N=" %12.0gc r(N) as result " k=" %12.0gc r(k)

if `t17_err' == 0 {
    display as result "Test 17 completed successfully"
    local passed_tests "`passed_tests' 17"
}
else {
    display as error "Test 17 failed: foreign detailed describe smoke test"
    local failed_tests "`failed_tests' 17"
}
timer off 17
timer list 17
display as text "Test 17 finished in" as result %6.2f r(t17) "s"

// Test Case 18: Metadata key scaffold (old test3 T1+T3 merged)
display _newline "=== TEST CASE 18: Metadata Key Scaffold ==="
timer clear 18
timer on 18
local ++total_tests
clear
set obs 30
gen long id = _n
gen str20 tag = "row_" + string(_n, "%03.0f")
capture dtparquet save "t1c18.parquet", replace
local t18_err 0
if _rc != 0 {
    display as error "Test 18 failed: save error " _rc
    local ++t18_err
}
else {
    plugin call dtparquet_plugin, "has_metadata_key" "t1c18.parquet" "dtparquet.dtmeta"
    if "`has_metadata_key'" != "1" local ++t18_err

    plugin call dtparquet_plugin, "has_metadata_key" "t1c18.parquet" "does.not.exist"
    if "`has_metadata_key'" != "0" local ++t18_err

    plugin call dtparquet_plugin, "has_metadata_key" "`foreign_large'" "dtparquet.dtmeta"
    if "`has_metadata_key'" != "0" local ++t18_err
}
if `t18_err' == 0 {
    display as result "Test 18 completed successfully"
    local passed_tests "`passed_tests' 18"
}
else {
    display as error "Test 18 failed: footer metadata key checks did not match"
    local failed_tests "`failed_tests' 18"
}
timer off 18
timer list 18
display as text "Test 18 finished in" as result %6.2f r(t18) "s"

// Cleanup
capture erase "t1c3.parquet"
capture erase "t1c3_source.dta"
capture erase "t1c3_roundtrip.dta"
capture erase "t1c4.parquet"
capture erase "t1c4_source.dta"
capture erase "t1c4_import.dta"
capture erase "t1c5.parquet"
capture erase "t1c5_source.dta"
capture erase "t1c6_isolation.parquet"
capture erase "t1c7.parquet.tmp"
capture erase "t1c7_dummy.parquet"
capture erase "t1c8.parquet"
capture erase "t1c8_source.dta"
capture erase "t1c8_import.dta"
capture erase "t1c9.parquet"
capture erase "t1c10.parquet"
capture erase "t1c11.parquet"
capture erase "t1c11_source.dta"
capture erase "t1c11_exp.parquet"
capture erase "t1c11_imp.dta"
capture erase "t1c12_noext.parquet"
capture erase "t1c12_upper.parquet"
capture erase "t1c14_base.dta"
capture erase "t1c14.parquet"
capture erase "t1c14_exp.parquet"
capture erase "t1c14_imp.dta"
capture erase "t1c15_opts.parquet"
capture erase "t1c16.parquet"
capture erase "t1c18.parquet"

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
