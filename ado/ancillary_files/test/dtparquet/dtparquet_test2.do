* dtparquet_test2.do
* Consolidated TYPES + METADATA suite for dtparquet.
* Covers: full metadata roundtrip (labels, value labels, dataset/var notes,
* load_meta counts), nolabel empties, value labels without a data label,
* partitioned metadata embedding, LF label text, native datetime roundtrip,
* UTF-8, empty/long strings, strL stress and temp-name fixtures, row-key
* collision guard, large-file strL scale, foreign categorical catmode matrix
* (encode/raw/both/validation + pandas/arrow fixtures), wide data, chunk
* boundary, GeoParquet geometry modes, and CRS metadata.
* Consolidates: old test1 T3/T11 (metadata, LF), old test5 T1-T3/T5b/T6/T7/T11
* (wide, boundary, UTF-8, strL, categorical, native), old test6 T8 (strings),
* old test7 T6b-T6h/T13-T15d (categorical, metadata, strL).
* Deduplicated away: old test2 T4 weak metadata spot check, old test5 T6/T7
* weaker categorical checks (covered by fixture-backed T6f-T6h copies), old
* test6 T9 BPOM smoke (covered by the exact-dimension check in C12).
* Run from the checkout root.
// Objective: Check data types, metadata, and GeoParquet geometry conversion.

version 16
clear all
macro drop _all
global fnum "02"
global fmain "dtparquet"
global ftype "test"
global vernum "v01"
global fname "${fnum}_${fmain}_${ftype}_${vernum}"
global logname "${ftype}_${vernum}"

capture confirm file "ado/dtparquet.ado"
if _rc != 0 {
    if c(hostname) == "NUXS" {
        cd d:/OneDrive/MyWork/00personal/stata/dtkit
    }
    else {
        cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
    }
}

log using ado/ancillary_files/test/log/dtparquet_test2.log, replace

timer clear 99
timer on 99

// Install local versions
discard
capture program drop dtparquet
run "ado/dtparquet.ado"
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/dtparquet.dll"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

cap program drop _cleanup_dir_shallow
program _cleanup_dir_shallow
    args target_dir
    if "`target_dir'" == "" {
        exit
    }
    local files : dir "`target_dir'" files "*"
    foreach f of local files {
        capture erase "`target_dir'/`f'"
    }
    local subdirs : dir "`target_dir'" dirs "*"
    foreach d of local subdirs {
        local subfiles : dir "`target_dir'/`d'" files "*"
        foreach sf of local subfiles {
            capture erase "`target_dir'/`d'/`sf'"
        }
        capture rmdir "`target_dir'/`d'"
    }
    capture rmdir "`target_dir'"
end

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

display _newline(2) "=========================================="
display "Starting dtparquet TYPES+METADATA Test Suite (consolidated)"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Full metadata roundtrip (old test7 T13 default branch)
display _newline "=== TEST CASE 1: Metadata Roundtrip ==="
timer clear 1
timer on 1
local ++total_tests
local meta_parquet "t2c1_meta.parquet"
capture erase "`meta_parquet'"

clear
set obs 3
gen byte z = _n
label define zlbl 1 "one" 2 "two" 3 "three", replace
label values z zlbl
label variable z "z label"
label data "meta roundtrip"
notes: "dataset note one"
notes z: "z var note one"

local t1_err 0
dtparquet save "`meta_parquet'", replace
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
if "`dtmeta_loaded'" != "1" local ++t1_err
if real("`dtmeta_dta_obs'") != 3 local ++t1_err
if real("`dtmeta_dta_vars'") != 1 local ++t1_err
if "`dtmeta_dta_ts'" == "" local ++t1_err
if real("`dtmeta_dta_note_count'") != 1 local ++t1_err
if real("`dtmeta_var_note_count'") != 1 local ++t1_err

dtparquet use using "`meta_parquet'", clear
local z_var_label_default : variable label z
local z_val_label_default : value label z
local d_label_default : data label
notes _count d_note_count_default : _dta
notes _count z_note_count_default : z
if "`z_var_label_default'" != "z label" local ++t1_err
if "`z_val_label_default'" != "zlbl" local ++t1_err
if "`d_label_default'" != "meta roundtrip" local ++t1_err
if `d_note_count_default' != 1 local ++t1_err
if `z_note_count_default' != 1 local ++t1_err

* Value-label content check (old test1 T3 strongest assertion)
local lbl1 : label zlbl 1
if "`lbl1'" != "one" local ++t1_err

if `t1_err' == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: metadata behavior not deterministic"
    local failed_tests "`failed_tests' 1"
}
timer off 1
timer list 1
display as text "Test 1 finished in" as result %6.2f r(t1) "s"

// Test Case 2: nolabel metadata empties (old test7 T13 nolabel branch)
display _newline "=== TEST CASE 2: nolabel Metadata Empties ==="
timer clear 2
timer on 2
local ++total_tests
local t2_err 0
dtparquet save "`meta_parquet'", replace nolabel
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
if "`dtmeta_loaded'" != "1" local ++t2_err
if real("`dtmeta_dta_obs'") != 0 local ++t2_err
if real("`dtmeta_dta_vars'") != 0 local ++t2_err
if "`dtmeta_dta_ts'" != "" local ++t2_err
if real("`dtmeta_dta_note_count'") != 0 local ++t2_err
if real("`dtmeta_var_note_count'") != 0 local ++t2_err

dtparquet use using "`meta_parquet'", clear
local z_var_label_nolabel : variable label z
local z_val_label_nolabel : value label z
local d_label_nolabel : data label
notes _count d_note_count_nolabel : _dta
notes _count z_note_count_nolabel : z
if "`z_var_label_nolabel'" != "" local ++t2_err
if "`z_val_label_nolabel'" != "" local ++t2_err
if "`d_label_nolabel'" != "" local ++t2_err
if `d_note_count_nolabel' != 0 local ++t2_err
if `z_note_count_nolabel' != 0 local ++t2_err

if `t2_err' == 0 {
    display as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    display as error "Test 2 failed: nolabel metadata not empty"
    local failed_tests "`failed_tests' 2"
}
timer off 2
timer list 2
display as text "Test 2 finished in" as result %6.2f r(t2) "s"

// Test Case 3: Value labels without data label (old test7 T13b)
display _newline "=== TEST CASE 3: Value Labels Without Data Label ==="
timer clear 3
timer on 3
local ++total_tests
local meta_nodlabel_parquet "t2c3_nodlabel.parquet"
capture erase "`meta_nodlabel_parquet'"

clear
set obs 2
gen byte src = _n - 1
label define vl_src 0 "No" 1 "Yes", replace
label values src vl_src
label variable src "Source test"

local t3_err 0
dtparquet save "`meta_nodlabel_parquet'", replace
plugin call dtparquet_plugin, "load_meta" "`meta_nodlabel_parquet'"
if "`dtmeta_loaded'" != "1" local ++t3_err
if real("`dtmeta_label_count'") != 2 local ++t3_err

dtparquet use using "`meta_nodlabel_parquet'", clear
local src_var_label_nodlabel : variable label src
local src_val_label_nodlabel : value label src
if "`src_var_label_nodlabel'" != "Source test" local ++t3_err
if "`src_val_label_nodlabel'" != "vl_src" local ++t3_err

tempvar src_text
decode src, gen(`src_text')
if `src_text'[1] != "No" local ++t3_err
if `src_text'[2] != "Yes" local ++t3_err

if `t3_err' == 0 {
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: value labels without data label not preserved"
    local failed_tests "`failed_tests' 3"
}
timer off 3
timer list 3
display as text "Test 3 finished in" as result %6.2f r(t3) "s"

// Test Case 4: Partitioned metadata embedding (old test7 T14)
display _newline "=== TEST CASE 4: Partitioned Metadata Embedding ==="
timer clear 4
timer on 4
local ++total_tests
local partition_meta_dir "t2c4_partmeta"
capture noisily _cleanup_dir_shallow "`partition_meta_dir'"

clear
set obs 10
gen byte group = (_n > 5)
gen byte z = _n
label define zlbl 1 "one" 2 "two" 3 "three", replace
label values z zlbl
label variable z "z label"
label data "partitioned meta test"

dtparquet save "`partition_meta_dir'", partitionby(group) replace

local leaf_file "`partition_meta_dir'/group=0"
local leaf_files : dir "`leaf_file'" files "*.parquet"
local first_leaf : word 1 of `leaf_files'
local leaf_path "`leaf_file'/`first_leaf'"

local t4_err 0
plugin call dtparquet_plugin, "load_meta" "`leaf_path'"
if "`dtmeta_loaded'" != "1" local ++t4_err
if "`dtmeta_dta_vars'" != "2" local ++t4_err
dtparquet use using "`leaf_path'", clear
local z_var_label : variable label z
if "`z_var_label'" != "z label" local ++t4_err

if `t4_err' == 0 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 failed: partitioned metadata embedding did not work"
    local failed_tests "`failed_tests' 4"
}
timer off 4
timer list 4
display as text "Test 4 finished in" as result %6.2f r(t4) "s"
capture noisily _cleanup_dir_shallow "`partition_meta_dir'"

// Test Case 5: LF label save/use roundtrip (old test1 T11)
display _newline "=== TEST CASE 5: LF Label Roundtrip ==="
timer clear 5
timer on 5
local ++total_tests
clear
set obs 2
generate byte v_lf = _n
// LF label text broke uselabel's line parser on save ("in not found", r(111)).
mata: st_vlmodify("pq_lf", (1\2), ("line1" + char(10) + "line2" \ "plain"))
label values v_lf pq_lf
local t5_err 0
capture dtparquet save "t2c5_lf.parquet", replace
if _rc {
    display as error "Test 5 Save failed with error " _rc
    local ++t5_err
}
else {
    clear
    capture dtparquet use using "t2c5_lf.parquet"
    if _rc {
        display as error "Test 5 Use failed with error " _rc
        local ++t5_err
    }
    else {
        mata: st_vlload("pq_lf", t5v=., t5t="")
        mata: st_numscalar("t5_n", rows(t5v))
        mata: st_numscalar("t5_at", strpos(t5t[1], char(10)))
        mata: st_numscalar("t5_len", strlen(t5t[1]))
        if t5_n != 2 | t5_at != 6 | t5_len != 11 {
            display as error "Test 5 failed: LF mapping not preserved (n=" t5_n " at=" t5_at " len=" t5_len ")"
            local ++t5_err
        }
        mata: st_local("t5_plain", t5t[2])
        if "`t5_plain'" != "plain" {
            display as error "Test 5 failed: second mapping mismatch"
            local ++t5_err
        }
    }
}
if `t5_err' == 0 {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed with `t5_err' error(s)"
    local failed_tests "`failed_tests' 5"
}
timer off 5
timer list 5
display as text "Test 5 finished in" as result %6.2f r(t5) "s"

// Test Case 6: Native datetime roundtrip, no epoch offset (old test5 T11)
display _newline "=== TEST CASE 6: Native Datetime Roundtrip ==="
timer clear 6
timer on 6
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

    dtparquet save "t2c6_native.parquet", replace
    clear
    dtparquet use using "t2c6_native.parquet"
    datasignature
    assert r(datasignature) == "`sig_orig'"
}
if _rc == 0 {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: native round-trip error"
    local failed_tests "`failed_tests' 6"
}
timer off 6
timer list 6
display as text "Test 6 finished in" as result %6.2f r(t6) "s"

// Test Case 7: UTF-8 special characters (old test5 T3)
display _newline "=== TEST CASE 7: UTF-8 Special Characters ==="
timer clear 7
timer on 7
local ++total_tests
clear
set obs 3
gen str80 s = ""
replace s = "Standard text" in 1
replace s = "Unicode: € £ ¥ ©" in 2
replace s = "Unicode: aeio" in 3
capture dtparquet save "t2c7_utf8.parquet", replace
if _rc != 0 {
    display as error "Test 7 failed: save error " _rc
    local failed_tests "`failed_tests' 7"
}
else {
    capture noisily {
        clear
        dtparquet use using "t2c7_utf8.parquet"
        assert s == "Standard text" in 1
        assert s == "Unicode: € £ ¥ ©" in 2
        assert s == "Unicode: aeio" in 3
    }
    if _rc == 0 {
        display as result "Test 7 completed successfully"
        local passed_tests "`passed_tests' 7"
    }
    else {
        display as error "Test 7 failed: assertion error"
        local failed_tests "`failed_tests' 7"
    }
}
timer off 7
timer list 7
display as text "Test 7 finished in" as result %6.2f r(t7) "s"

// Test Case 8: Empty/long strings (old test6 T8)
display _newline "=== TEST CASE 8: Empty/Long Strings ==="
timer clear 8
timer on 8
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
dtparquet save "t2c8_complex.parquet", replace

dtparquet u "t2c8_complex.parquet", cle
if _rc == 0 {
    if substr(long_str[2],1,1) == "A" & length(long_str[2]) > 90 & str_col[2] == "" {
        display as result "Test 8 completed successfully"
        local passed_tests "`passed_tests' 8"
    }
    else {
        display as error "Test 8 failed: data mismatch"
        local failed_tests "`failed_tests' 8"
    }
}
else {
    display as error "Test 8 failed: rc=" _rc
    local failed_tests "`failed_tests' 8"
}
timer off 8
timer list 8
display as text "Test 8 finished in" as result %6.2f r(t8) "s"

// Test Case 9: strL stress case (old test5 T5b)
display _newline "=== TEST CASE 9: strL Stress Case ==="
timer clear 9
timer on 9
local ++total_tests
clear
set obs 10
gen id = _n
gen strL long_str = ""
replace long_str = "Short string" in 1
replace long_str = "Longer string with some content" in 2
replace long_str = "Very " + c(alpha) + " long string " + c(ALPHA) + " to test strL limits" in 3
forvalues i = 4/10 {
    replace long_str = "Row `i' data " + string(`i'^2) in `i'
}
datasignature
local sig9_orig = r(datasignature)

capture dtparquet save "t2c9_strl.parquet", replace
if _rc != 0 {
    display as error "Test 9 failed: save error " _rc
    local failed_tests "`failed_tests' 9"
}
else {
    capture noisily {
        clear
        dtparquet use using "t2c9_strl.parquet"
        datasignature
        assert r(datasignature) == "`sig9_orig'"
        assert long_str != "" in 3
    }
    if _rc == 0 {
        display as result "Test 9 completed successfully"
        local passed_tests "`passed_tests' 9"
    }
    else {
        display as error "Test 9 failed: assertion or signature mismatch"
        local failed_tests "`failed_tests' 9"
    }
}
timer off 9
timer list 9
display as text "Test 9 finished in" as result %6.2f r(t9) "s"

// strL fixtures regenerate deterministically when missing, so these cases also
// work in a fresh checkout (copied from old test7). Only missing files are
// written. Polars imports only when a fixture is missing.
local strl_datadir "ado/ancillary_files/test/dtparquet/data"
local strl_collide "`strl_datadir'/strl_collide.parquet"
local strl_clean "`strl_datadir'/strl_clean.parquet"
capture noisily python:
from sfi import Macro
from pathlib import Path
outdir = Path(Macro.getLocal("strl_datadir"))
n = 20
missing = [p for p in ("strl_collide.parquet", "strl_clean.parquet") if not (outdir / p).exists()]
if missing:
    import polars as pl
    frames = {
    "strl_collide.parquet": {
    "id": list(range(1, n + 1)),
    "A_B": [f"n{i}" for i in range(1, n + 1)],
    "A-B": pl.Series("A-B", [b"xyz-value-%d" % i for i in range(1, n + 1)], dtype=pl.Binary),
    "C-D": pl.Series("C-D", [None] * n, dtype=pl.Binary),
    "E-B": pl.Series("E-B", [b"e-value-%d" % i for i in range(1, n + 1)], dtype=pl.Binary),
    "G_H": pl.Series("G_H", [None] * n, dtype=pl.Binary),
    "E_F": [f"e{i}" for i in range(1, n + 1)],
    },
    "strl_clean.parquet": {
    "id": list(range(1, n + 1)),
    "A-B": pl.Series("A-B", [b"xyz-value-%d" % i for i in range(1, n + 1)], dtype=pl.Binary),
    "C-D": pl.Series("C-D", [None] * n, dtype=pl.Binary),
    "E-B": pl.Series("E-B", [b"e-value-%d" % i for i in range(1, n + 1)], dtype=pl.Binary),
    "G_H": pl.Series("G_H", [None] * n, dtype=pl.Binary),
    "E_F": [f"e{i}" for i in range(1, n + 1)],
    },
    }
    for _name in missing:
        pl.DataFrame(frames[_name]).write_parquet(outdir / _name)
end
capture confirm file "`strl_collide'"
if _rc != 0 {
    display as error "strL fixture strl_collide.parquet missing and regeneration failed (Stata Python needs polars)"
    log close
    exit 601
}
capture confirm file "`strl_clean'"
if _rc != 0 {
    display as error "strL fixture strl_clean.parquet missing and regeneration failed (Stata Python needs polars)"
    log close
    exit 601
}

// Test Case 10: strL temp collision keeps master values (old test7 T15)
display _newline "=== TEST CASE 10: strL Temp Name Collision ==="
timer clear 10
timer on 10
local ++total_tests
local t10_err 0
clear
capture dtparquet use using "`strl_collide'", clear
if _rc != 0 {
    display as error "Test 10 failed: load rc=" _rc
    local ++t10_err
}
else {
    if c(k) != 6 | c(N) != 20 {
        display as error "Test 10 failed: expected 6x20, got " c(k) "x" c(N)
        local ++t10_err
    }
    quietly ds
    if "`r(varlist)'" != "id A_B E_F C_D E_B G_H" {
        display as error "Test 10 failed: order is `r(varlist)'"
        local ++t10_err
    }
    foreach _c10 in "A_B str12" "C_D str1" "E_B str10" "G_H strL" {
        gettoken _vn10 _vt10 : _c10
        local _vt10 = trim("`_vt10'")
        local _t10: type `_vn10'
        if "`_t10'" != "`_vt10'" {
            display as error "Test 10 failed: `_vn10' is `_t10', expected `_vt10'"
            local ++t10_err
        }
    }
    capture assert A_B == "n" + string(_n)
    if _rc {
        display as error "Test 10 failed: collision did not keep master values"
        local ++t10_err
    }
    capture assert E_B == "e-value-" + string(_n)
    if _rc {
        display as error "Test 10 failed: merged strL values mismatch"
        local ++t10_err
    }
    quietly count if G_H != ""
    if r(N) != 0 {
        display as error "Test 10 failed: omitted strL column not all missing"
        local ++t10_err
    }
    datasignature
    if "`r(datasignature)'" != "20:6(86203):3986759359:4145686844" {
        display as error "Test 10 failed: datasignature is `r(datasignature)'"
        local ++t10_err
    }
}
if `t10_err' == 0 {
    display as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    local failed_tests "`failed_tests' 10"
}
timer off 10
timer list 10
display as text "Test 10 finished in" as result %6.2f r(t10) "s"

// Test Case 11: clean strL temp names (old test7 T15b)
display _newline "=== TEST CASE 11: Clean strL Temp Names ==="
timer clear 11
timer on 11
local ++total_tests
local t11_err 0
clear
capture dtparquet use using "`strl_clean'", clear
if _rc != 0 {
    display as error "Test 11 failed: load rc=" _rc
    local ++t11_err
}
else {
    if c(k) != 6 | c(N) != 20 {
        display as error "Test 11 failed: expected 6x20, got " c(k) "x" c(N)
        local ++t11_err
    }
    quietly ds
    if "`r(varlist)'" != "id E_F A_B C_D E_B G_H" {
        display as error "Test 11 failed: order is `r(varlist)'"
        local ++t11_err
    }
    local _t11_ab: type A_B
    if "`_t11_ab'" != "str12" {
        display as error "Test 11 failed: A_B is `_t11_ab'"
        local ++t11_err
    }
    capture assert A_B == "xyz-value-" + string(_n)
    if _rc {
        display as error "Test 11 failed: merged strL values mismatch"
        local ++t11_err
    }
    local _t11_gh: type G_H
    if "`_t11_gh'" != "strL" {
        display as error "Test 11 failed: G_H is `_t11_gh'"
        local ++t11_err
    }
    quietly count if G_H != ""
    if r(N) != 0 {
        display as error "Test 11 failed: omitted strL column not all missing"
        local ++t11_err
    }
    datasignature
    if "`r(datasignature)'" != "20:6(86233):818548423:4048667132" {
        display as error "Test 11 failed: datasignature is `r(datasignature)'"
        local ++t11_err
    }
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

// Test Case 12: strL variable named like the internal row key (old test7 T15c)
display _newline "=== TEST CASE 12: Row-Key Name Collision ==="
timer clear 12
timer on 12
local ++total_tests
local t12_err 0
clear
set obs 20
gen long id = _n
gen strL _dtparquet_strl_key = "kval" + string(_n)
gen str20 E_F = "e"
capture dtparquet save "t2c12_keyfreak.parquet", replace
if _rc != 0 {
    display as error "Test 12 failed: save rc=" _rc
    local ++t12_err
}
else {
    clear
    capture dtparquet use using "t2c12_keyfreak.parquet", clear
    if _rc != 198 {
        display as error "Test 12 failed: expected rc=198, got " _rc
        local ++t12_err
    }
}
capture erase "t2c12_keyfreak.parquet"
if `t12_err' == 0 {
    display as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    local failed_tests "`failed_tests' 12"
}
timer off 12
timer list 12
display as text "Test 12 finished in" as result %6.2f r(t12) "s"

// Test Case 13: large foreign file delivers every strL variable (old test7 T15d)
display _newline "=== TEST CASE 13: All-Missing strL at Scale ==="
timer clear 13
timer on 13
local ++total_tests
local t13_err 0
clear
capture dtparquet use using "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear
if _rc != 0 {
    display as error "Test 13 failed: load rc=" _rc
    local ++t13_err
}
else {
    if c(k) != 61 | c(N) != 343788 {
        display as error "Test 13 failed: expected 61x343788, got " c(k) "x" c(N)
        local ++t13_err
    }
    quietly ds, has(type strL)
    if wordcount("`r(varlist)'") != 16 {
        display as error "Test 13 failed: expected 16 strL vars"
        local ++t13_err
    }
    quietly ds
    local _names13 `r(varlist)'
    local _hit13: list posof "IMPORTER" in _names13
    if `_hit13' == 0 {
        display as error "Test 13 failed: omitted strL variable IMPORTER missing"
        local ++t13_err
    }
    else {
        local _t13: type IMPORTER
        if "`_t13'" != "strL" {
            display as error "Test 13 failed: IMPORTER is `_t13'"
            local ++t13_err
        }
    }
}
if `t13_err' == 0 {
    display as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
}
else {
    local failed_tests "`failed_tests' 13"
}
timer off 13
timer list 13
display as text "Test 13 finished in" as result %6.2f r(t13) "s"

// Test Case 14: Foreign categorical mapping is deterministic (old test7 T6b)
display _newline "=== TEST CASE 14: Foreign Categorical Mapping ==="
timer clear 14
timer on 14
local ++total_tests
clear
set obs 4
gen str5 cat = ""
replace cat = "red" in 1
replace cat = "blue" in 2
replace cat = "red" in 3
replace cat = "green" in 4
_apply_foreign_cat_labels cat, mode(encode)
local cat_vallab: value label cat
local t14_err 0
capture confirm numeric variable cat
if _rc != 0 local ++t14_err
if "`cat_vallab'" != "cat_1" local ++t14_err
if cat[1] != 3 local ++t14_err
if cat[2] != 1 local ++t14_err
if cat[3] != 3 local ++t14_err
if cat[4] != 2 local ++t14_err
tempvar cat_text
decode cat, gen(`cat_text')
if `cat_text'[1] != "red" local ++t14_err
if `cat_text'[2] != "blue" local ++t14_err
if `cat_text'[3] != "red" local ++t14_err
if `cat_text'[4] != "green" local ++t14_err

if `t14_err' == 0 {
    display as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
}
else {
    display as error "Test 14 failed: foreign categorical mapping not deterministic"
    local failed_tests "`failed_tests' 14"
}
timer off 14
timer list 14
display as text "Test 14 finished in" as result %6.2f r(t14) "s"

// Test Case 15: catmode raw/both/validation (old test7 T6c-T6e merged)
display _newline "=== TEST CASE 15: Catmode raw/both/validation ==="
timer clear 15
timer on 15
local ++total_tests
local t15_err 0
dtparquet use PRODUCT_ID using "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/500, clear catmode(raw)
local product_id_type_raw: type PRODUCT_ID
local product_id_vallab_raw: value label PRODUCT_ID
if substr("`product_id_type_raw'", 1, 3) != "str" local ++t15_err
if "`product_id_vallab_raw'" != "" local ++t15_err

clear
set obs 4
gen str5 catboth = ""
replace catboth = "red" in 1
replace catboth = "blue" in 2
replace catboth = "red" in 3
replace catboth = "green" in 4
_apply_foreign_cat_labels catboth, mode(both)
local catboth_id_vallab: value label catboth_id
capture confirm variable catboth_id
if _rc != 0 local ++t15_err
capture confirm numeric variable catboth_id
if _rc != 0 local ++t15_err
if "`catboth_id_vallab'" != "cat_1" local ++t15_err
tempvar catboth_id_text
decode catboth_id, gen(`catboth_id_text')
if `catboth_id_text'[1] != catboth[1] local ++t15_err
if `catboth_id_text'[2] != catboth[2] local ++t15_err
if `catboth_id_text'[3] != catboth[3] local ++t15_err
if `catboth_id_text'[4] != catboth[4] local ++t15_err

capture dtparquet use PRODUCT_ID using "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear catmode(invalid)
if _rc != 198 local ++t15_err

if `t15_err' == 0 {
    display as result "Test 15 completed successfully"
    local passed_tests "`passed_tests' 15"
}
else {
    display as error "Test 15 failed: catmode(raw/both/validation) mismatch"
    local failed_tests "`failed_tests' 15"
}
timer off 15
timer list 15
display as text "Test 15 finished in" as result %6.2f r(t15) "s"

// Test Case 16: Fixture-backed categorical modes (old test7 T6f-T6h)
display _newline "=== TEST CASE 16: Fixture-Backed Categorical Modes ==="
timer clear 16
timer on 16
local ++total_tests
local foreign_pandas "ado/ancillary_files/test/dtparquet/data/foreign_cat_pandas.parquet"
local foreign_arrow "ado/ancillary_files/test/dtparquet/data/foreign_cat_arrow_dict.parquet"
local t16_err 0

dtparquet use cat using "`foreign_pandas'", clear catmode(encode)
count
if r(N) != 4 local ++t16_err
capture confirm numeric variable cat
if _rc != 0 local ++t16_err
local cat_vallab_foreign: value label cat
if "`cat_vallab_foreign'" != "cat_1" local ++t16_err
tempvar cat_foreign_text
decode cat, gen(`cat_foreign_text')
count if missing(`cat_foreign_text')
if r(N) != 0 local ++t16_err

dtparquet use cat using "`foreign_arrow'", clear catmode(raw)
local cat_type_raw_fixture: type cat
local cat_vallab_raw_fixture: value label cat
count
if r(N) != 4 local ++t16_err
if substr("`cat_type_raw_fixture'", 1, 3) != "str" local ++t16_err
if "`cat_vallab_raw_fixture'" != "" local ++t16_err
count if missing(cat)
if r(N) != 0 local ++t16_err
levelsof cat, local(cat_raw_fixture_levels)
local cat_raw_fixture_level_n : word count `cat_raw_fixture_levels'
if `cat_raw_fixture_level_n' != 3 local ++t16_err

dtparquet use cat using "`foreign_arrow'", clear catmode(both)
local cat_id_vallab_fixture: value label cat_id
count
if r(N) != 4 local ++t16_err
capture confirm variable cat_id
if _rc != 0 local ++t16_err
capture confirm numeric variable cat_id
if _rc != 0 local ++t16_err
if "`cat_id_vallab_fixture'" != "cat_1" local ++t16_err
tempvar cat_id_fixture_text
decode cat_id, gen(`cat_id_fixture_text')
if `cat_id_fixture_text'[1] != cat[1] local ++t16_err
if `cat_id_fixture_text'[2] != cat[2] local ++t16_err
if `cat_id_fixture_text'[3] != cat[3] local ++t16_err
if `cat_id_fixture_text'[4] != cat[4] local ++t16_err

if `t16_err' == 0 {
    display as result "Test 16 completed successfully"
    local passed_tests "`passed_tests' 16"
}
else {
    display as error "Test 16 failed: fixture-backed categorical modes mismatch"
    local failed_tests "`failed_tests' 16"
}
timer off 16
timer list 16
display as text "Test 16 finished in" as result %6.2f r(t16) "s"

// Test Case 17: Wide data, row-major check (old test5 T1)
display _newline "=== TEST CASE 17: Wide Data ==="
timer clear 17
timer on 17
local ++total_tests
clear
set obs 100
gen id = _n
forvalues i = 1/200 {
    gen var`i' = _n * `i'
}
capture dtparquet save "t2c17_wide.parquet", replace
if _rc != 0 {
    display as error "Test 17 failed: save error " _rc
    local failed_tests "`failed_tests' 17"
}
else {
    capture noisily {
        clear
        dtparquet use using "t2c17_wide.parquet"
        assert c(k) == 201
        assert c(N) == 100
        assert var200 == id * 200
    }
    if _rc == 0 {
        display as result "Test 17 completed successfully"
        local passed_tests "`passed_tests' 17"
    }
    else {
        display as error "Test 17 failed: assertion error"
        local failed_tests "`failed_tests' 17"
    }
}
timer off 17
timer list 17
display as text "Test 17 finished in" as result %6.2f r(t17) "s"

// Test Case 18: Chunk boundary, N not multiple of chunksize (old test5 T2)
display _newline "=== TEST CASE 18: Chunk Boundary ==="
timer clear 18
timer on 18
local ++total_tests
clear
set obs 55
gen id = _n
capture dtparquet save "t2c18_boundary.parquet", replace
if _rc != 0 {
    display as error "Test 18 failed: save error " _rc
    local failed_tests "`failed_tests' 18"
}
else {
    capture noisily {
        clear
        dtparquet use using "t2c18_boundary.parquet"
        assert c(N) == 55
    }
    if _rc == 0 {
        display as result "Test 18 completed successfully"
        local passed_tests "`passed_tests' 18"
    }
    else {
        display as error "Test 18 failed: assertion error"
        local failed_tests "`failed_tests' 18"
    }
}
timer off 18
timer list 18
display as text "Test 18 finished in" as result %6.2f r(t18) "s"

// * geoparquet fixture
// Use a point, a rectangle, and a null geometry. PyArrow is required.
python:
import json
import struct
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sfi import Macro

geo_tmp = tempfile.TemporaryDirectory(prefix="dtkit-geoparquet-")
geo_file = Path(geo_tmp.name) / "geometry.parquet"
Macro.setLocal("geo_file", geo_file.as_posix())
bounds = ["xmin", "ymin", "xmax", "ymax"]
geo_meta = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {
        "encoding": "WKB",
        "geometry_types": ["Point", "Polygon"],
        "covering": {"bbox": {n: ["bbox", n] for n in bounds}},
    }},
}
geo_schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("geometry", pa.binary(), metadata={
        "ARROW:extension:name": "geoarrow.wkb",
        "ARROW:extension:metadata": "{}",
    }),
    pa.field("bbox", pa.struct([(n, pa.float64()) for n in bounds])),
], metadata={"geo": json.dumps(geo_meta)})
point = struct.pack("<BIdd", 1, 1, 1, 2)
rectangle = struct.pack("<BIII10d", 1, 3, 1, 5,
                        0, 0, 4, 0, 4, 2, 0, 2, 0, 0)
pq.write_table(pa.Table.from_pydict({
    "id": [1, 2, 3],
    "geometry": [point, rectangle, None],
    "bbox": [dict(zip(bounds, b)) if b else None
             for b in [(1, 2, 1, 2), (0, 0, 4, 2), None]],
}, schema=geo_schema), geo_file)
end

// * geoparquet text and metadata
display _newline "=== TEST CASE 19: GeoParquet Text and Metadata ==="
local ++total_tests
capture noisily {
    dtparquet use using "`geo_file'", clear
    assert c(N) == 3 & c(k) == 2
    assert id == _n
    assert geometry[1] == "POINT (1 2)"
    assert geometry[2] == "POLYGON ((0 0, 4 0, 4 2, 0 2, 0 0))"
    assert geometry[3] == ""
    local geo_type : type geometry
    assert "`geo_type'" == "strL"
    assert "`: char _dta[geoparquet_version]'" == "1.1.0"
    assert "`: char _dta[geoparquet_primary]'" == "geometry"
    assert "`: char _dta[geoparquet_encoding]'" == "WKB"
    assert "`: char _dta[geoparquet_crs]'" == "OGC:CRS84"
    assert "`: char _dta[geoparquet_mode]'" == "wkt"

    dtparquet use using "`geo_file'", clear geometry(hex)
    assert geometry[1] == "0101000000000000000000F03F0000000000000040"
    assert geometry[3] == ""
    assert "`: char _dta[geoparquet_mode]'" == "hex"
}
if _rc == 0 {
    display as result "Test 19 completed successfully"
    local passed_tests "`passed_tests' 19"
}
else {
    display as error "Test 19 failed: GeoParquet text or metadata mismatch"
    local failed_tests "`failed_tests' 19"
}

// * geoparquet coordinates and geometry removal
display _newline "=== TEST CASE 20: GeoParquet Coordinates and Drop ==="
local ++total_tests
capture noisily {
    dtparquet use using "`geo_file'", clear geometry(centroid bbox)
    assert c(N) == 3 & c(k) == 7
    assert _CX[1] == 1 & _CY[1] == 2
    assert _CX[2] == 2 & _CY[2] == 1
    assert _xmin[1] == 1 & _ymin[1] == 2
    assert _xmax[1] == 1 & _ymax[1] == 2
    assert _xmin[2] == 0 & _ymin[2] == 0
    assert _xmax[2] == 4 & _ymax[2] == 2
    foreach v in _CX _CY _xmin _ymin _xmax _ymax {
        assert missing(`v'[3])
    }

    dtparquet use using "`geo_file'" if id == 2, clear ///
        geometry(wkt centroid bbox)
    assert c(N) == 1 & c(k) == 8
    assert id == 2 & _CX == 2 & _CY == 1
    assert geometry == "POLYGON ((0 0, 4 0, 4 2, 0 2, 0 0))"

    dtparquet use using "`geo_file'", clear geometry(drop)
    assert c(N) == 3 & c(k) == 1
    assert id == _n
}
if _rc == 0 {
    display as result "Test 20 completed successfully"
    local passed_tests "`passed_tests' 20"
}
else {
    display as error "Test 20 failed: GeoParquet coordinates or drop mismatch"
    local failed_tests "`failed_tests' 20"
}

// * invalid geometry options
display _newline "=== TEST CASE 21: Invalid Geometry Options ==="
local ++total_tests
local t21_err 0
foreach spec in invalid "wkt hex" {
    capture noisily dtparquet use using "`geo_file'", clear geometry(`spec')
    if _rc != 198 local ++t21_err
}
if `t21_err' == 0 {
    display as result "Test 21 completed successfully"
    local passed_tests "`passed_tests' 21"
}
else {
    display as error "Test 21 failed: expected r(198) for invalid geometry()"
    local failed_tests "`failed_tests' 21"
}
python: geo_tmp.cleanup()

// Cleanup
capture erase "t2c1_meta.parquet"
capture erase "t2c3_nodlabel.parquet"
capture erase "t2c5_lf.parquet"
capture erase "t2c6_native.parquet"
capture erase "t2c7_utf8.parquet"
capture erase "t2c8_complex.parquet"
capture erase "t2c9_strl.parquet"
capture erase "t2c17_wide.parquet"
capture erase "t2c18_boundary.parquet"

timer off 99
capture timer list 99
local elapsed = r(t99)
if `elapsed' < 60 {
    display as result "Total elapsed time: " %9.2f `elapsed' " seconds"
}
else if `elapsed' < 3600 {
    display as result "Total elapsed time: " %9.2f (`elapsed'/60) " minutes (" %9.2f `elapsed' " seconds)"
}
else {
    display as result "Total elapsed time: " %9.2f (`elapsed'/3600) " hours (" %9.2f (`elapsed'/60) " minutes)"
}

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
}
exit, clear
