* dtparquet_test3.do
* Consolidated PLUGIN + PERFORMANCE suite for dtparquet, with a gated BENCH block.
* Covers: plugin setup_check and contract failure paths, describe macro
* contract, schema protocol mismatch, read-path schema handoff, timer() mode
* matrix, in-range reads, if-qualifier pushdown, allstring casts, save/read-back
* roundtrip, partition_by + overwrite guard, sql_if filtering, compress codecs
* and guards, export/import quoted paths, chunksize boundaries, streaming
* export/import with datasignature, pool lifecycle, typed transfer metrics,
* single-pass lazy execution, streaming activation, adaptive batch autotuning,
* producer-consumer pipeline, strL arena path, execution boundaries.
* BENCH block (stress loops, dtparquet-vs-pq scenarios, pihps repro+matrix)
* runs only when the global DTPARQUET_BENCH is 1:
*     global DTPARQUET_BENCH 1
*     do ado/ancillary_files/test/dtparquet/dtparquet_test3.do
* Consolidates: old test4 (streaming), old test5 (row-major/perf metrics), old
* test7 plugin/partition/compress/export sections, old test8 (stress), old
* dtparquet_vs_pq.do, old dtparquet_test9.do (pihps), old
* dtparquet_installed_benchmark.do.
* Deduplicated away: repeated streaming label/notes spot checks (covered
* strongly in test2 C1), repeated tiny/large chunksize copies (one boundary
* case kept), 1M installed benchmark as a separate file (superseded by the
* vs-pq scenario block), second copies of export/import and categorical checks.
* Run from the checkout root.

version 16
clear all
set more off

capture confirm file "ado/dtparquet.ado"
if _rc != 0 {
    if c(hostname) == "NUXS" {
        cd d:/OneDrive/MyWork/00personal/stata/dtkit
    }
    else {
        cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
    }
}

capture log close _all
log using ado/ancillary_files/test/log/dtparquet_test3.log, replace

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

cap program drop _cleanup_dir_recursive
program _cleanup_dir_recursive
    args target_dir
    if "`target_dir'" == "" {
        exit
    }
    local subdirs : dir "`target_dir'" dirs "*"
    foreach d of local subdirs {
        capture noisily _cleanup_dir_recursive "`target_dir'/`d'"
    }
    local files : dir "`target_dir'" files "*"
    foreach f of local files {
        capture erase "`target_dir'/`f'"
    }
    capture rmdir "`target_dir'"
end

* Deterministic pre-clean for generated outputs from prior interrupted runs
capture erase "t3c4_badproto.parquet"
capture erase "t3c10_roundtrip.parquet"
capture erase "t3c12_filtered.parquet"
capture erase "t3c13_zstd.parquet"
capture erase "t3c13_uncompressed.parquet"
capture erase "t3c13_default.parquet"
capture erase "t3c13_level_bad.parquet"
capture erase "t3c13_bad.parquet"
capture erase "t3c17_stream.parquet"
capture erase "t3c17_restored.dta"
capture erase "t3c18_tiny.parquet"
capture erase "t3c18_tiny_restored.dta"
capture erase "t3c18_large.parquet"
capture erase "t3c18_large_restored.dta"
capture erase "t3c18_chunk.parquet"
capture erase "t3c21_lazy.parquet"
capture erase "t3c22_streaming_trigger.parquet"
capture erase "t3c23_autotune.parquet"
capture erase "t3c24_pipeline.parquet"
capture erase "t3c25_strl.parquet"
capture erase "t3c26_boundaries.parquet"
local _pre_parts : dir "." dirs "t3c11_part_*"
foreach _d of local _pre_parts {
    capture noisily _cleanup_dir_recursive "`_d'"
}
capture noisily _cleanup_dir_shallow "t3c16 export import tmp"

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

local bpom "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet"

display _newline(2) "=========================================="
display "Starting dtparquet PLUGIN+PERF Test Suite (consolidated)"
display "Timestamp: " c(current_date) " " c(current_time)
display "Bench block: ${DTPARQUET_BENCH}"
display "==========================================" _newline

// Test Case 1: Plugin loads (old test7 T1)
display _newline "=== TEST CASE 1: Plugin loads ==="
timer clear 1
timer on 1
local ++total_tests
capture plugin call dtparquet_plugin, "setup_check"
if _rc == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: plugin did not load, rc=" _rc
    local failed_tests "`failed_tests' 1"
}
timer off 1
capture timer list 1
display as text "Test 1 finished in " as result %4.2f r(t1) "s"

// Test Case 2: Plugin contract failure paths are deterministic (old test7 T1b)
display _newline "=== TEST CASE 2: Plugin Contract Failure Paths ==="
timer clear 2
timer on 2
local ++total_tests
local t2_err 0
capture plugin call dtparquet_plugin, "unknown_subfunction"
if _rc != 198 local ++t2_err

capture plugin call dtparquet_plugin, "describe"
if _rc != 198 local ++t2_err

capture plugin call dtparquet_plugin, "save"
if _rc != 198 local ++t2_err

capture plugin call dtparquet_plugin, "read"
if _rc != 198 local ++t2_err

capture plugin call dtparquet_plugin, "has_metadata_key"
if _rc != 198 local ++t2_err

capture plugin call dtparquet_plugin, "load_meta"
if _rc != 198 local ++t2_err

if `t2_err' == 0 {
    display as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    display as error "Test 2 failed: plugin contract failure paths not deterministic"
    local failed_tests "`failed_tests' 2"
}
timer off 2
capture timer list 2
display as text "Test 2 finished in " as result %4.2f r(t2) "s"

// Test Case 3: Describe contract macros from plugin (old test7 T2)
display _newline "=== TEST CASE 3: Describe Contract Macros ==="
timer clear 3
timer on 3
local ++total_tests
plugin call dtparquet_plugin, "describe" "`bpom'" "0" "0" "0" "0" "1" "0"
local t3_err 0
if real("`n_rows'") <= 0 local ++t3_err
if real("`n_columns'") <= 0 local ++t3_err
if "`name_1'" == "" local ++t3_err
if "`type_1'" == "" local ++t3_err
if "`polars_type_1'" == "" local ++t3_err
if real("`string_length_1'") < 0 local ++t3_err
if real("`schema_protocol_version'") != 2 local ++t3_err
if `"`schema_payload'"' == "" local ++t3_err

if `t3_err' == 0 {
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: describe macro contract not available"
    local failed_tests "`failed_tests' 3"
}
timer off 3
capture timer list 3
display as text "Test 3 finished in " as result %4.2f r(t3) "s"

// Test Case 4: Schema protocol mismatch is explicit (old test7 T2b)
display _newline "=== TEST CASE 4: Schema Protocol Mismatch ==="
timer clear 4
timer on 4
local ++total_tests
clear
set obs 2
gen long id = _n
local bad_schema_payload `"{""protocol_version"":999,""fields"":[{""name"":""id"",""dtype"":""long"",""format"":""%12.0g"",""str_length"":0}]}"'
capture plugin call dtparquet_plugin, "save" "t3c4_badproto.parquet" "id" "0" "0" "" "`bad_schema_payload'" "" "zstd" "-1" "1" "0" "1" "1000"
local t4_err 0
if _rc != 198 local ++t4_err
capture confirm file "t3c4_badproto.parquet"
if _rc == 0 local ++t4_err

if `t4_err' == 0 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 failed: protocol mismatch did not fail fast"
    local failed_tests "`failed_tests' 4"
}
timer off 4
capture timer list 4
display as text "Test 4 finished in " as result %4.2f r(t4) "s"

// Test Case 5: Read path with ado pre-read setup (old test7 T3)
display _newline "=== TEST CASE 5: Read Path Schema Handoff ==="
timer clear 5
timer on 5
local ++total_tests
dtparquet use using "`bpom'" in 1/50000, clear timer(plugin)
local t5_err 0
if inlist("$read_schema_handoff", "json_v2", "legacy_macros") == 0 local ++t5_err
count
if r(N) != 50000 local ++t5_err

if `t5_err' == 0 {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed: read path did not execute correctly"
    local failed_tests "`failed_tests' 5"
}
timer off 5
capture timer list 5
display as text "Test 5 finished in " as result %4.2f r(t5) "s"

// Test Case 6: Unified timer() mode matrix (old test7 T3b)
display _newline "=== TEST CASE 6: timer() Mode Matrix ==="
timer clear 6
timer on 6
local ++total_tests
local t6_err 0
foreach tmode in off stata plugin all {
    capture noisily dtparquet use using "`bpom'" in 1/50, clear timer(`tmode')
    if _rc != 0 local ++t6_err
    foreach rs in plugin_ms strl_fix_ms meta_ms cat_ms describe_ms loadmeta_ms varprep_ms mapping_ms filevars_ms matchwin_ms genrecast_ms readfields_ms castjson_ms loaded_rows plugin_timers {
        capture scalar __rcheck = r(`rs')
        if _rc != 0 local ++t6_err
        else if inlist("`rs'", "loaded_rows") {
            if (__rcheck <= 0) local ++t6_err
        }
        else {
            if (missing(__rcheck) | __rcheck < 0) local ++t6_err
        }
    }
    capture local rfile = r(file)
    if _rc != 0 local ++t6_err
    else if `"`rfile'"' == "" local ++t6_err
    local exp_plugin = inlist("`tmode'", "plugin", "all")
    if (r(plugin_timers) != `exp_plugin') local ++t6_err
}

if `t6_err' == 0 {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: timer() modes did not map correctly"
    local failed_tests "`failed_tests' 6"
}
timer off 6
capture timer list 6
display as text "Test 6 finished in " as result %4.2f r(t6) "s"

// Test Case 7: Subset varlist + in-range paths (old test7 T4+T5 merged)
display _newline "=== TEST CASE 7: Varlist + In-Range Read ==="
timer clear 7
timer on 7
local ++total_tests
local t7_err 0
dtparquet use ID PRODUCT_ID fetchdate using "`bpom'", clear
if c(k) != 3 local ++t7_err
count
if r(N) <= 0 local ++t7_err

dtparquet use using "`bpom'" in 1/100, clear
count
if r(N) != 100 local ++t7_err

if `t7_err' == 0 {
    display as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: varlist subset or in-range read did not work"
    local failed_tests "`failed_tests' 7"
}
timer off 7
capture timer list 7
display as text "Test 7 finished in " as result %4.2f r(t7) "s"

// Test Case 8: If-qualifier pushdown path (old test7 T5b)
display _newline "=== TEST CASE 8: If-Qualifier Pushdown ==="
timer clear 8
timer on 8
local ++total_tests
dtparquet use year using "`bpom'" if year > 2015 in 1/2000, clear
local t8_err 0
count
if r(N) <= 0 local ++t8_err
if r(N) > 2000 local ++t8_err
summ year, meanonly
if r(min) <= 2015 local ++t8_err
if "$if_filter_mode" != "expr" local ++t8_err

if `t8_err' == 0 {
    display as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: if-qualifier pushdown did not work"
    local failed_tests "`failed_tests' 8"
}
timer off 8
capture timer list 8
display as text "Test 8 finished in " as result %4.2f r(t8) "s"

// Test Case 9: Allstring int64 cast (old test7 T6)
display _newline "=== TEST CASE 9: Allstring int64 Cast ==="
timer clear 9
timer on 9
local ++total_tests
dtparquet use ID year using "`bpom'", clear allstring
local id_type: type ID
local year_type: type year
if "`id_type'" == "strL" & "`year_type'" == "strL" {
    display as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}
else {
    display as error "Test 9 failed: allstring int64 cast did not work"
    local failed_tests "`failed_tests' 9"
}
timer off 9
capture timer list 9
display as text "Test 9 finished in " as result %4.2f r(t9) "s"

// Test Case 10: Save and read back through dtparquet (old test7 T7)
display _newline "=== TEST CASE 10: Save and Read-Back Roundtrip ==="
timer clear 10
timer on 10
local ++total_tests
local roundtrip_file "t3c10_roundtrip.parquet"
capture erase "`roundtrip_file'"

dtparquet use ID PRODUCT_ID year using "`bpom'" in 1/1000, clear
local id_first = ID[1]
local id_last = ID[_N]
local year_first = year[1]

dtparquet save "`roundtrip_file'", replace timer(plugin)
local t10_err 0
capture confirm file "`roundtrip_file'"
if _rc != 0 local ++t10_err
if inlist("$write_schema_handoff", "json_v2", "legacy_macros") == 0 local ++t10_err

dtparquet use using "`roundtrip_file'", clear
count
if r(N) != 1000 local ++t10_err
if c(k) != 3 local ++t10_err
if ID[1] != `id_first' local ++t10_err
if ID[_N] != `id_last' local ++t10_err
if year[1] != `year_first' local ++t10_err

if `t10_err' == 0 {
    display as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    display as error "Test 10 failed: save and read-back roundtrip did not work"
    local failed_tests "`failed_tests' 10"
}
timer off 10
capture timer list 10
display as text "Test 10 finished in " as result %4.2f r(t10) "s"

// Test Case 11: Plugin save with partition_by + overwrite guard (old test7 T8)
display _newline "=== TEST CASE 11: Partition_by + Overwrite Guard ==="
timer clear 11
timer on 11
local ++total_tests
local partition_dir "t3c11_part_test"
capture noisily _cleanup_dir_recursive "`partition_dir'"
capture error 0

dtparquet use ID PRODUCT_ID year using "`bpom'" in 1/1000, clear
quietly ds
local varlist `r(varlist)'
local var_count: word count `varlist'

local i = 0
foreach vari of local varlist {
    local i = `i' + 1
    local typei: type `vari'
    local formati: format `vari'
    local str_length 0

    if ((substr("`typei'", 1, 3) == "str") & (lower("`typei'") != "strl")) {
        local str_length = substr("`typei'", 4, .)
        local typei string
    }

    local name_`i' `vari'
    local dtype_`i' `typei'
    local format_`i' `formati'
    local str_length_`i' `str_length'
}

local t11_err 0
plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "1" "0" "0"
if _rc != 0 local ++t11_err

local partition_dirs : dir "`partition_dir'" dirs "year=*"
local partition_dir_n : word count `partition_dirs'
if `partition_dir_n' <= 0 local ++t11_err

capture plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "0" "0" "0"
if _rc == 0 local ++t11_err

if `t11_err' == 0 {
    display as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}
else {
    display as error "Test 11 failed: partition_by save + overwrite guard did not work"
    local failed_tests "`failed_tests' 11"
}
timer off 11
capture timer list 11
display as text "Test 11 finished in " as result %4.2f r(t11) "s"
capture noisily _cleanup_dir_recursive "`partition_dir'"

// Test Case 12: Plugin save sql_if filter semantics (old test7 T9)
display _newline "=== TEST CASE 12: Save sql_if Filtering ==="
timer clear 12
timer on 12
local ++total_tests
local filtered_file "t3c12_filtered.parquet"
capture erase "`filtered_file'"
capture error 0

plugin call dtparquet_plugin, "save" "`filtered_file'" "from_macro" "0" "0" "year > 2015" "from_macros" "" "zstd" "-1" "1" "0" "0"
local rc_test12_save = _rc
local t12_err 0
if (`rc_test12_save' != 0) local ++t12_err
capture confirm file "`filtered_file'"
if _rc != 0 local ++t12_err

dtparquet use using "`filtered_file'", clear
count
if r(N) <= 0 local ++t12_err
if year[1] <= 2015 local ++t12_err
summ year, meanonly
if r(min) <= 2015 local ++t12_err

if `t12_err' == 0 {
    display as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    display as error "Test 12 failed: save sql_if filtering did not work"
    local failed_tests "`failed_tests' 12"
}
timer off 12
capture timer list 12
display as text "Test 12 finished in " as result %4.2f r(t12) "s"

// Test Case 13: Compression codec/default semantics (old test7 T9b)
display _newline "=== TEST CASE 13: Compression Codec Semantics ==="
timer clear 13
timer on 13
local ++total_tests
local compress_zstd "t3c13_zstd.parquet"
local compress_uncompressed "t3c13_uncompressed.parquet"
local compress_default "t3c13_default.parquet"
local compress_level_bad "t3c13_level_bad.parquet"
local compress_bad "t3c13_bad.parquet"

capture erase "`compress_zstd'"
capture erase "`compress_uncompressed'"
capture erase "`compress_default'"
capture erase "`compress_level_bad'"
capture erase "`compress_bad'"

local t13_err 0
dtparquet save "`compress_zstd'", replace compress(zstd)
if _rc != 0 local ++t13_err
capture confirm file "`compress_zstd'"
if _rc != 0 local ++t13_err

dtparquet save "`compress_uncompressed'", replace compress(uncompressed)
if _rc != 0 local ++t13_err
capture confirm file "`compress_uncompressed'"
if _rc != 0 local ++t13_err

dtparquet save "`compress_default'", replace
if _rc != 0 local ++t13_err
capture confirm file "`compress_default'"
if _rc != 0 local ++t13_err

capture dtparquet save "`compress_bad'", replace compress(invalid_codec)
if _rc != 198 local ++t13_err

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "zstd" "3" "1" "0" "0"
if _rc != 198 local ++t13_err

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "snappy" "1" "1" "0" "0"
if _rc != 198 local ++t13_err

dtparquet use using "`compress_zstd'", clear
count
if r(N) <= 0 local ++t13_err

dtparquet use using "`compress_uncompressed'", clear
count
if r(N) <= 0 local ++t13_err

dtparquet use using "`compress_default'", clear
count
if r(N) <= 0 local ++t13_err

capture dtparquet save "`compress_bad'", replace compress_string_to_numeric
if _rc != 198 local ++t13_err

if `t13_err' == 0 {
    display as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
}
else {
    display as error "Test 13 failed: compress() codec/default semantics not deterministic"
    local failed_tests "`failed_tests' 13"
}
timer off 13
capture timer list 13
display as text "Test 13 finished in " as result %4.2f r(t13) "s"

// Test Case 14: Metadata key on the filtered file (old test7 T10)
display _newline "=== TEST CASE 14: Metadata Key on Filtered File ==="
timer clear 14
timer on 14
local ++total_tests
plugin call dtparquet_plugin, "has_metadata_key" "`filtered_file'" "dtparquet.dtmeta"
if "`has_metadata_key'" == "1" {
    display as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
}
else {
    display as error "Test 14 failed: metadata key scaffold not present"
    local failed_tests "`failed_tests' 14"
}
timer off 14
capture timer list 14
display as text "Test 14 finished in " as result %4.2f r(t14) "s"

// Test Case 15: Export/import quoted paths + allstring (old test7 T11)
display _newline "=== TEST CASE 15: Export/Import Quoted Paths ==="
timer clear 15
timer on 15
local ++total_tests
local export_dir "t3c16 export import tmp"
capture mkdir "`export_dir'"

local source_dta "`export_dir'/source labels.dta"
local export_parquet "`export_dir'/quoted path.parquet"
local import_default_dta "`export_dir'/import default.dta"
local import_allstring_dta "`export_dir'/import allstring fixture.dta"

capture erase "`source_dta'"
capture erase "`export_parquet'"
capture erase "`import_default_dta'"
capture erase "`import_allstring_dta'"

clear
set obs 3
gen long id = _n
gen double code = 1234567890120 + _n
gen str8 grp = cond(mod(_n, 2) == 0, "beta", "alpha")
label define grp_lbl 1 "alpha" 2 "beta", replace
gen byte grp_code = cond(grp == "alpha", 1, 2)
label values grp_code grp_lbl
label variable grp_code "group code"
quietly save "`source_dta'", replace

local t15_err 0
dtparquet export "`export_parquet'" using "`source_dta'"
capture confirm file "`export_parquet'"
if _rc != 0 local ++t15_err

capture dtparquet export "`export_parquet'" using "`source_dta'"
if _rc == 0 local ++t15_err

dtparquet import "`import_default_dta'" using "`export_parquet'"
quietly use "`import_default_dta'", clear
count
if r(N) != 3 local ++t15_err
if c(k) != 4 local ++t15_err
if id[1] != 1 local ++t15_err
if code[3] != 1234567890123 local ++t15_err

dtparquet export "`export_parquet'" using "`source_dta'", replace nolabel
dtparquet import "`import_allstring_dta'" using "`export_parquet'", replace nolabel
quietly use "`import_allstring_dta'", clear
count
if r(N) != 3 local ++t15_err
if c(k) != 4 local ++t15_err
if id[1] != 1 local ++t15_err
if code[3] != 1234567890123 local ++t15_err

dtparquet import "`import_allstring_dta'" using "`bpom'", replace allstring
quietly use "`import_allstring_dta'", clear
local id15_type: type ID
local year15_type: type year
if "`id15_type'" != "strL" local ++t15_err
if "`year15_type'" != "strL" local ++t15_err

if `t15_err' == 0 {
    display as result "Test 15 completed successfully"
    local passed_tests "`passed_tests' 15"
}
else {
    display as error "Test 15 failed: export/import command paths did not work"
    local failed_tests "`failed_tests' 15"
}
timer off 15
capture timer list 15
display as text "Test 15 finished in " as result %4.2f r(t15) "s"
capture erase "`source_dta'"
capture erase "`export_parquet'"
capture erase "`import_default_dta'"
capture erase "`import_allstring_dta'"
capture noisily _cleanup_dir_shallow "`export_dir'"

// Shared streaming fixture (old test4 setup, expanded auto)
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
local expected_obs = c(N)

// Test Case 16: Streaming export/import with datasignature (old test4 T1-T3+T6)
display _newline "=== TEST CASE 16: Streaming Export/Import ==="
timer clear 16
timer on 16
local ++total_tests
local t16_err 0
dtparquet export "t3c17_stream.parquet" using "`original_dta'", replace chunksize(1000)
if _rc != 0 {
    display as error "Test 16 failed: streaming export rc=" _rc
    local ++t16_err
}
else {
    capture confirm file "t3c17_stream.parquet"
    if _rc != 0 {
        display as error "Test 16 failed: output file not created"
        local ++t16_err
    }
    else {
        dtparquet import "t3c17_restored.dta" using "t3c17_stream.parquet", replace chunksize(750)
        if _rc != 0 {
            display as error "Test 16 failed: streaming import rc=" _rc
            local ++t16_err
        }
        else {
            use "t3c17_restored.dta", clear
            datasignature
            if "`baseline'" != "`r(datasignature)'" {
                display as error "Test 16 failed: DataSignature mismatch"
                local ++t16_err
            }
            if c(N) != `expected_obs' {
                display as error "Test 16 failed: N=" c(N) ", expected `expected_obs'"
                local ++t16_err
            }
        }
    }
}
if `t16_err' == 0 {
    display as result "Test 16 completed successfully"
    local passed_tests "`passed_tests' 16"
}
else {
    local failed_tests "`failed_tests' 16"
}
timer off 16
capture timer list 16
display as text "Test 16 finished in" as result %6.2f r(t16) "s"

// Test Case 17: Chunksize boundaries + override (old test4 T8/T9 + test5 T4)
display _newline "=== TEST CASE 17: Chunksize Boundaries ==="
timer clear 17
timer on 17
local ++total_tests
local t17_err 0
use "`original_dta'", clear
dtparquet export "t3c18_tiny.parquet" using "`original_dta'", replace chunksize(10)
if _rc != 0 {
    display as error "Test 17 failed: tiny-chunk export rc=" _rc
    local ++t17_err
}
else {
    dtparquet import "t3c18_tiny_restored.dta" using "t3c18_tiny.parquet", replace chunksize(7)
    if _rc != 0 {
        display as error "Test 17 failed: tiny-chunk import rc=" _rc
        local ++t17_err
    }
    else {
        use "t3c18_tiny_restored.dta", clear
        datasignature
        if "`baseline'" != "`r(datasignature)'" {
            display as error "Test 17 failed: DataSignature mismatch with tiny chunks"
            local ++t17_err
        }
    }
}
use "`original_dta'", clear
dtparquet export "t3c18_large.parquet" using "`original_dta'", replace chunksize(10000)
if _rc != 0 {
    display as error "Test 17 failed: large-chunk export rc=" _rc
    local ++t17_err
}
else {
    dtparquet import "t3c18_large_restored.dta" using "t3c18_large.parquet", replace chunksize(10000)
    if _rc != 0 {
        display as error "Test 17 failed: large-chunk import rc=" _rc
        local ++t17_err
    }
    else {
        use "t3c18_large_restored.dta", clear
        if c(N) != `expected_obs' {
            display as error "Test 17 failed: large-chunk N=" c(N)
            local ++t17_err
        }
    }
}
clear
set obs 10
gen x = _n
capture dtparquet save "t3c18_chunk.parquet", replace chunksize(1)
if _rc != 0 {
    display as error "Test 17 failed: chunksize(1) save rc=" _rc
    local ++t17_err
}
else {
    capture noisily {
        clear
        dtparquet use using "t3c18_chunk.parquet", chunksize(1)
        assert x == _n
    }
    if _rc != 0 {
        display as error "Test 17 failed: chunksize(1) roundtrip assertion error"
        local ++t17_err
    }
}
if `t17_err' == 0 {
    display as result "Test 17 completed successfully"
    local passed_tests "`passed_tests' 17"
}
else {
    local failed_tests "`failed_tests' 17"
}
timer off 17
capture timer list 17
display as text "Test 17 finished in" as result %6.2f r(t17) "s"

// Test Case 18: Data signature fidelity (old test5 T5)
display _newline "=== TEST CASE 18: Data Signature Fidelity ==="
timer clear 18
timer on 18
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
datasignature
local sig18_orig = r(datasignature)

capture dtparquet save "t3c18_sig.parquet", replace
if _rc != 0 {
    display as error "Test 18 failed: save error " _rc
    local failed_tests "`failed_tests' 18"
}
else {
    capture noisily {
        clear
        dtparquet use using "t3c18_sig.parquet"
        datasignature
        assert r(datasignature) == "`sig18_orig'"
    }
    if _rc == 0 {
        display as result "Test 18 completed successfully"
        local passed_tests "`passed_tests' 18"
    }
    else {
        display as error "Test 18 failed: datasignature mismatch"
        local failed_tests "`failed_tests' 18"
    }
}
timer off 18
timer list 18
display as text "Test 18 finished in" as result %6.2f r(t18) "s"
capture erase "t3c18_sig.parquet"

// Test Case 19: T03 pool lifecycle metrics (old test5 T12)
display _newline "=== TEST CASE 19: Pool Lifecycle Metrics ==="
timer clear 19
timer on 19
local ++total_tests
local t19_n_rows 50000
local t19_chunk_size 5000
tempfile t19_pool_stub

capture noisily {
    clear
    set obs `t19_n_rows'
    gen long id = _n
    forvalues j = 1/16 {
        gen double x`j' = runiform() * `j'
    }

    dtparquet save "`t19_pool_stub'", replace chunksize(`t19_chunk_size') timer(plugin)

    local t19_parquet_file "`t19_pool_stub'.parquet"
    local t19_compute_after_save = real("$compute_pool_inits")
    local t19_io_after_save = real("$io_pool_inits")
    assert !missing(`t19_compute_after_save')
    assert !missing(`t19_io_after_save')
    assert `t19_compute_after_save' >= 1
    assert `t19_io_after_save' >= 1

    clear
    dtparquet use using "`t19_parquet_file'", clear chunksize(`t19_chunk_size') timer(plugin)
    local t19_compute_after_use1 = real("$compute_pool_inits")
    local t19_io_after_use1 = real("$io_pool_inits")
    local t19_planned_batches = real("$read_planned_batches")
    local t19_processed_batches = real("$read_processed_batches")
    assert `t19_compute_after_use1' == `t19_compute_after_save'
    assert `t19_io_after_use1' == `t19_io_after_save'
    assert `t19_planned_batches' > 1
    assert `t19_processed_batches' > 1

    clear
    dtparquet use using "`t19_parquet_file'", clear chunksize(`t19_chunk_size') timer(plugin)
    local t19_compute_after_use2 = real("$compute_pool_inits")
    local t19_io_after_use2 = real("$io_pool_inits")
    assert `t19_compute_after_use2' == `t19_compute_after_save'
    assert `t19_io_after_use2' == `t19_io_after_save'
}

if _rc == 0 {
    display as result "Test 19 completed successfully"
    local passed_tests "`passed_tests' 19"
}
else {
    display as error "Test 19 failed: pool lifecycle metrics mismatch"
    local failed_tests "`failed_tests' 19"
}
timer off 19
timer list 19
display as text "Test 19 finished in" as result %6.2f r(t19) "s"

// Test Case 20: T05 typed transfer writer metrics (old test5 T13)
display _newline "=== TEST CASE 20: Typed Transfer Metrics ==="
timer clear 20
timer on 20
local ++total_tests
local t20_n_rows 10000
local t20_chunk_size 2500
tempfile t20_transfer_stub

capture noisily {
    clear
    set obs `t20_n_rows'
    gen byte flag = mod(_n, 2)
    gen int small = mod(_n, 1000)
    gen long id = _n
    gen float xf = _n / 100
    gen double xd = (_n / 10) + runiform()
    gen double td = mdy(1, 1, 2020) + _n
    format td %td
    gen double tc = clock("01jan2020 00:00:00", "DMYhms") + (_n * 1000)
    format tc %tc
    gen str24 code = "c_" + string(_n, "%06.0f")
    gen strL note = "row_" + string(_n) + "_abcdefghijklmnopqrstuvwxyz0123456789"

    dtparquet save "`t20_transfer_stub'", replace chunksize(`t20_chunk_size') timer(plugin)

    clear
    dtparquet use using "`t20_transfer_stub'.parquet", clear chunksize(`t20_chunk_size') timer(plugin)

    assert _N == `t20_n_rows'
    assert c(k) == 9

    local t20_fallback = real("$read_fallback_calls")
    local t20_conversion_failures = real("$read_conversion_failures")
    local t20_number_calls = real("$read_replace_number_calls")
    local t20_string_calls = real("$read_replace_string_calls")

    assert !missing(`t20_fallback')
    assert !missing(`t20_conversion_failures')
    assert !missing(`t20_number_calls')
    assert !missing(`t20_string_calls')
    assert `t20_number_calls' > 0
    assert `t20_string_calls' > 0
    assert `t20_fallback' == 0
    assert `t20_conversion_failures' == 0
}

if _rc == 0 {
    display as result "Test 20 completed successfully"
    local passed_tests "`passed_tests' 20"
}
else {
    display as error "Test 20 failed: typed transfer metrics mismatch"
    local failed_tests "`failed_tests' 20"
}
timer off 20
timer list 20
display as text "Test 20 finished in" as result %6.2f r(t20) "s"

// Test Case 21: T06 single-pass lazy execution + out-of-range (old test5 T14/T14b)
display _newline "=== TEST CASE 21: Single-Pass Lazy Execution ==="
timer clear 21
timer on 21
local ++total_tests
local t21_n_rows 120000
local t21_chunk_size 4000

capture noisily {
    clear
    set obs `t21_n_rows'
    gen long id = _n
    gen byte grp = mod(_n, 3)
    gen double value = _n * 1.5
    gen str16 tag = "r_" + string(_n, "%06.0f")

    dtparquet save "t3c21_lazy.parquet", replace chunksize(`t21_chunk_size') timer(plugin)

    clear
    dtparquet use id grp value tag using "t3c21_lazy.parquet" if grp > 0 in 1/`t21_n_rows', clear chunksize(`t21_chunk_size') timer(plugin)

    count
    assert r(N) == 80000
    assert c(k) == 4
    assert id[1] == 1
    assert id[_N] == 119999

    local t21_collect_calls = real("$read_collect_calls")
    local t21_planned_batches = real("$read_planned_batches")
    local t21_processed_batches = real("$read_processed_batches")

    assert `t21_collect_calls' == 1
    assert `t21_planned_batches' == `t21_processed_batches'
    assert `t21_planned_batches' > 1
    assert "$read_lazy_mode" == "single_pass"
    assert "$read_streaming_enabled" == "0"
}

if _rc == 0 {
    display as result "Test 21 completed successfully"
    local passed_tests "`passed_tests' 21"
}
else {
    display as error "Test 21 failed: single-pass lazy execution mismatch"
    local failed_tests "`failed_tests' 21"
}
timer off 21
timer list 21
display as text "Test 21 finished in" as result %6.2f r(t21) "s"

// Test Case 21b: out-of-range in-window diagnostics (old test5 T14b)
timer clear 22
timer on 22
local ++total_tests
capture noisily {
    clear
    dtparquet use id grp value tag using "t3c21_lazy.parquet" in 1000001/1100000, clear chunksize(`t21_chunk_size') timer(plugin)

    count
    assert r(N) == 0
    assert "$read_streaming_enabled" == "0"
}

if _rc == 0 {
    display as result "Test 21b completed successfully"
    local passed_tests "`passed_tests' 21b"
}
else {
    display as error "Test 21b failed: streaming diagnostics mismatch"
    local failed_tests "`failed_tests' 21b"
}
timer off 22
timer list 22
display as text "Test 21b finished in" as result %6.2f r(t22) "s"

// Test Case 22: T06c streaming activation, >1M requested rows (old test5 T14c)
display _newline "=== TEST CASE 22: Streaming Activation (>1M Rows) ==="
timer clear 23
timer on 23
local ++total_tests

capture noisily {
    local t22_n_rows = 1001000

    clear
    set obs `t22_n_rows'
    gen long id = _n
    gen byte grp = mod(_n, 5)

    dtparquet save "t3c22_streaming_trigger.parquet", replace chunksize(20000) timer(plugin)

    clear
    dtparquet use id grp using "t3c22_streaming_trigger.parquet" if id >= 1, clear chunksize(20000) timer(plugin)

    count
    assert r(N) == `t22_n_rows'
    assert "$read_streaming_enabled" == "1"
}

if _rc == 0 {
    display as result "Test 22 completed successfully"
    local passed_tests "`passed_tests' 22"
}
else {
    display as error "Test 22 failed: streaming activation mismatch"
    local failed_tests "`failed_tests' 22"
}
timer off 23
timer list 23
display as text "Test 22 finished in" as result %6.2f r(t23) "s"

// Test Case 23: T07 adaptive batch autotuning metrics (old test5 T15)
display _newline "=== TEST CASE 23: Adaptive Batch Autotuning ==="
timer clear 24
timer on 24
local ++total_tests
local t23_n_rows 150000
local t23_save_chunk 3000
local t23_use_chunk 2500

capture noisily {
    clear
    set obs `t23_n_rows'
    gen long id = _n
    forvalues j = 1/20 {
        gen double x`j' = runiform() * `j'
    }
    forvalues j = 1/10 {
        gen str32 s`j' = "v" + string(mod(_n * `j', 100000), "%05.0f")
    }

    dtparquet save "t3c23_autotune.parquet", replace chunksize(`t23_save_chunk') timer(plugin)

    local t23_write_selected = real("$write_selected_batch_size")
    local t23_write_row_width = real("$write_batch_row_width_bytes")
    local t23_write_memory_cap = real("$write_batch_memory_cap_rows")

    assert !missing(`t23_write_selected')
    assert !missing(`t23_write_row_width')
    assert !missing(`t23_write_memory_cap')
    assert `t23_write_selected' > 0
    assert `t23_write_row_width' > 0
    assert `t23_write_memory_cap' > 0
    assert `t23_write_selected' <= `t23_save_chunk'
    assert `t23_write_selected' <= `t23_write_memory_cap'
    assert inlist("$write_batch_tuner_mode", "adaptive", "fixed")

    clear
    dtparquet use using "t3c23_autotune.parquet", clear chunksize(`t23_use_chunk') timer(plugin)

    assert _N == `t23_n_rows'

    local t23_read_selected = real("$read_selected_batch_size")
    local t23_read_row_width = real("$read_batch_row_width_bytes")
    local t23_read_memory_cap = real("$read_batch_memory_cap_rows")

    assert !missing(`t23_read_selected')
    assert !missing(`t23_read_row_width')
    assert !missing(`t23_read_memory_cap')
    assert `t23_read_selected' > 0
    assert `t23_read_row_width' > 0
    assert `t23_read_memory_cap' > 0
    assert `t23_read_selected' <= `t23_use_chunk'
    assert `t23_read_selected' <= `t23_read_memory_cap'
    assert inlist("$read_batch_tuner_mode", "adaptive", "fixed")
}

if _rc == 0 {
    display as result "Test 23 completed successfully"
    local passed_tests "`passed_tests' 23"
}
else {
    display as error "Test 23 failed: adaptive batch autotuning mismatch"
    local failed_tests "`failed_tests' 23"
}
timer off 24
timer list 24
display as text "Test 23 finished in" as result %6.2f r(t24) "s"

// Test Case 24: T08 producer-consumer write pipeline metrics (old test5 T16)
display _newline "=== TEST CASE 24: Producer-Consumer Pipeline ==="
timer clear 25
timer on 25
local ++total_tests
local t24_n_rows 180000
local t24_chunk_size 2000

capture noisily {
    clear
    set obs `t24_n_rows'
    gen long id = _n
    forvalues j = 1/16 {
        gen double d`j' = runiform() * `j'
    }
    forvalues j = 1/8 {
        gen str40 s`j' = "q" + string(mod(_n * `j', 100000), "%05.0f")
    }

    dtparquet save "t3c24_pipeline.parquet", replace chunksize(`t24_chunk_size') timer(plugin)

    local t24_mode "$write_pipeline_mode"
    local t24_capacity = real("$write_queue_capacity")
    local t24_peak = real("$write_queue_peak")
    local t24_backpressure = real("$write_queue_bp_events")
    local t24_produced = real("$write_queue_prod_batches")
    local t24_consumed = real("$write_queue_cons_batches")
    local t24_processed = real("$write_processed_batches")

    assert inlist("`t24_mode'", "producer_consumer", "legacy_direct")

    if "`t24_mode'" == "producer_consumer" {
        assert !missing(`t24_capacity')
        assert !missing(`t24_peak')
        assert !missing(`t24_backpressure')
        assert !missing(`t24_produced')
        assert !missing(`t24_consumed')
        assert !missing(`t24_processed')
        assert `t24_capacity' >= 1
        assert `t24_peak' >= 1
        assert `t24_peak' <= `t24_capacity'
        assert `t24_produced' >= 1
        assert `t24_consumed' >= 1
        assert `t24_produced' == `t24_consumed'
        assert `t24_consumed' == `t24_processed'
    }
    else {
        assert missing(`t24_capacity') | `t24_capacity' == 0
        assert missing(`t24_peak') | `t24_peak' == 0
        assert missing(`t24_backpressure') | `t24_backpressure' >= 0
    }

    clear
    dtparquet use using "t3c24_pipeline.parquet", clear
    assert _N == `t24_n_rows'
}

if _rc == 0 {
    display as result "Test 24 completed successfully"
    local passed_tests "`passed_tests' 24"
}
else {
    display as error "Test 24 failed: producer-consumer write pipeline mismatch"
    local failed_tests "`failed_tests' 24"
}
timer off 25
timer list 25
display as text "Test 24 finished in" as result %6.2f r(t25) "s"

// Test Case 25: T10 strL arena path diagnostics (old test5 T17)
display _newline "=== TEST CASE 25: strL Arena Path ==="
timer clear 26
timer on 26
local ++total_tests
local t25_n_rows 12000

capture noisily {
    clear
    set obs `t25_n_rows'
    gen long id = _n
    gen strL payload = ""
    forvalues j = 1/32 {
        replace payload = payload + "abcdefghijklmnopqrstuvwxyz0123456789"
    }
    replace payload = payload + "_" + string(_n, "%08.0f")

    dtparquet save "t3c25_strl.parquet", replace chunksize(4000) timer(plugin)

    local t25_pull_strl = real("$write_pull_strl_calls")
    local t25_trunc_events = real("$write_strl_trunc_events")
    local t25_binary_events = real("$write_strl_binary_events")

    assert !missing(`t25_pull_strl')
    assert !missing(`t25_trunc_events')
    assert !missing(`t25_binary_events')
    assert `t25_pull_strl' == `t25_n_rows'
    assert `t25_trunc_events' == 0
    assert `t25_binary_events' == 0

    clear
    dtparquet use id payload using "t3c25_strl.parquet", clear
    count
    assert r(N) == `t25_n_rows'
    assert length(payload[1]) > 1100
}

if _rc == 0 {
    display as result "Test 25 completed successfully"
    local passed_tests "`passed_tests' 25"
}
else {
    display as error "Test 25 failed: strL arena path mismatch"
    local failed_tests "`failed_tests' 25"
}
timer off 26
timer list 26
display as text "Test 25 finished in" as result %6.2f r(t26) "s"

// Test Case 26: T12 execution boundaries (old test5 T18)
display _newline "=== TEST CASE 26: Execution Boundaries ==="
timer clear 27
timer on 27
local ++total_tests

capture noisily {
    clear
    set obs 2000
    gen long id = _n
    gen double value = _n * 1.25
    gen str20 label = "row_" + string(_n, "%06.0f")

    dtparquet save "t3c26_boundaries.parquet", replace chunksize(700) timer(plugin)
    assert inlist("$write_engine_stage", "execute", "stata_sink")

    clear
    dtparquet use using "t3c26_boundaries.parquet", clear chunksize(700) timer(plugin)
    count
    assert r(N) == 2000
    assert inlist("$read_engine_stage", "execute", "stata_sink")
}

if _rc == 0 {
    display as result "Test 26 completed successfully"
    local passed_tests "`passed_tests' 26"
}
else {
    display as error "Test 26 failed: execution boundary mismatch"
    local failed_tests "`failed_tests' 26"
}
timer off 27
timer list 27
display as text "Test 26 finished in" as result %6.2f r(t27) "s"

// Cleanup perf artifacts (bench block below manages its own files)
capture erase "t3c4_badproto.parquet"
capture erase "t3c10_roundtrip.parquet"
capture erase "t3c12_filtered.parquet"
capture erase "t3c13_zstd.parquet"
capture erase "t3c13_uncompressed.parquet"
capture erase "t3c13_default.parquet"
capture erase "t3c13_level_bad.parquet"
capture erase "t3c13_bad.parquet"
capture erase "t3c17_stream.parquet"
capture erase "t3c17_restored.dta"
capture erase "t3c18_tiny.parquet"
capture erase "t3c18_tiny_restored.dta"
capture erase "t3c18_large.parquet"
capture erase "t3c18_large_restored.dta"
capture erase "t3c18_chunk.parquet"
capture erase "t3c21_lazy.parquet"
capture erase "t3c22_streaming_trigger.parquet"
capture erase "t3c23_autotune.parquet"
capture erase "t3c24_pipeline.parquet"
capture erase "t3c25_strl.parquet"
capture erase "t3c26_boundaries.parquet"

// ============================================================
// BENCH BLOCK (old test8 + vs_pq + test9, gated on DTPARQUET_BENCH==1)
// Enable with: global DTPARQUET_BENCH 1
// ============================================================
if "${DTPARQUET_BENCH}" == "1" {
    display _newline(2) "=========================================="
    display "BENCH BLOCK (gated, DTPARQUET_BENCH=1)"
    display "=========================================="

    // Bench 1: setup_check stress (old test8 T1)
    display _newline "=== BENCH 1: setup_check stress (1000x) ==="
    timer clear 40
    timer on 40
    local ++total_tests
    local setup_fail_at 0
    forvalues i = 1/1000 {
        capture noisily plugin call dtparquet_plugin, "setup_check"
        if _rc != 0 {
            local setup_fail_at `i'
            continue, break
        }
    }
    if `setup_fail_at' == 0 {
        display as result "Bench 1 completed successfully (1000 setup_check iterations)"
        local passed_tests "`passed_tests' B1"
    }
    else {
        display as error "Bench 1 failed: setup_check failed at iteration `setup_fail_at'"
        local failed_tests "`failed_tests' B1"
    }
    timer off 40
    timer list 40

    // Bench 2: save/use roundtrip stress, 200 x 10k rows (old test8 T2)
    display _newline "=== BENCH 2: save/use stress (200 x 10k rows) ==="
    timer clear 41
    timer on 41
    local ++total_tests
    local io_fail_at 0
    forvalues i = 1/200 {
        quietly clear
        quietly set obs 10000
        quietly gen long id = _n
        quietly gen double x = _n * 1.125 + 7
        quietly gen str20 s = "row_" + string(_n)

        capture noisily dtparquet save "t3bench_roundtrip.parquet", replace
        if _rc != 0 {
            local io_fail_at `i'
            continue, break
        }

        capture noisily {
            clear
            dtparquet use using "t3bench_roundtrip.parquet"
            assert c(N) == 10000
            assert c(k) == 3
            assert id[1] == 1
            assert id[_N] == 10000
            assert s[1] == "row_1"
        }
        if _rc != 0 {
            local io_fail_at `i'
            continue, break
        }
    }
    capture erase "t3bench_roundtrip.parquet"
    if `io_fail_at' == 0 {
        display as result "Bench 2 completed successfully (200 save/use iterations)"
        local passed_tests "`passed_tests' B2"
    }
    else {
        display as error "Bench 2 failed: roundtrip failed at iteration `io_fail_at'"
        local failed_tests "`failed_tests' B2"
    }
    timer off 41
    timer list 41

    // Bench 3: dtparquet vs pq scenarios (old dtparquet_vs_pq.do, compacted)
    capture which pq
    if _rc != 0 {
        display as text "Bench 3 skipped: pq command not installed (ssc install pq)"
    }
    else {
        display _newline "=== BENCH 3: dtparquet vs pq (3 scenarios) ==="
        timer clear 42
        timer on 42
        local ++total_tests
        local bench_warmup 2
        local bench_measured 8
        local bench_total = `bench_warmup' + `bench_measured'
        local bench_chunk 50000

        tempfile bench_results
        tempname bench_post
        postfile `bench_post' str20 scenario str10 command str8 operation ///
            int run byte warmup double elapsed long src_nobs int src_nvars ///
            long out_nobs int out_nvars ///
            using "`bench_results'", replace

        foreach scenario in narrow_numeric wide_mixed string_heavy {
            display as text "Running scenario: `scenario'"
            clear
            set seed 12345

            if "`scenario'" == "narrow_numeric" {
                quietly set obs 500000
                generate long id = _n
                forvalues j = 1/6 {
                    generate long int`j' = mod(_n + `j', 1000000)
                }
                forvalues j = 1/6 {
                    generate double dbl`j' = (runiform() * 10000) + `j'
                }
            }
            else if "`scenario'" == "wide_mixed" {
                quietly set obs 100000
                generate long id = _n
                forvalues j = 1/80 {
                    generate long i`j' = mod((_n * `j') + 17, 1000000)
                }
                forvalues j = 1/60 {
                    generate double d`j' = (runiform() * 1000) + `j'
                }
                forvalues j = 1/40 {
                    generate str24 s`j' = "g" + string(mod(_n + `j', 10000), "%04.0f")
                }
            }
            else if "`scenario'" == "string_heavy" {
                quietly set obs 40000
                generate long id = _n
                generate str32 code = "id_" + string(_n, "%08.0f")
                generate str120 text_short = "entry_" + string(mod(_n, 97)) + "_" + string(mod(_n, 991))
                quietly generate strL text_long = ""
                forvalues j = 1/70 {
                    quietly replace text_long = text_long + "abcdefghijklmnopqrstuvwxzy0123456789"
                }
                replace text_long = text_long + "_" + string(_n, "%08.0f")
                forvalues j = 1/10 {
                    generate str80 note`j' = "note_" + string(mod(_n * `j', 100000), "%06.0f")
                }
                forvalues j = 1/6 {
                    generate double v`j' = runiform() * (`j' + 1)
                }
            }

            local src_nobs = _N
            local src_nvars = c(k)

            tempfile bench_source bench_dt bench_pq
            quietly save "`bench_source'", replace
            local dtparquet_file "`bench_dt'.parquet"
            local pq_file "`bench_pq'_pq.parquet"

            foreach cmd in dtparquet pq {
                foreach op in save use {
                    forvalues run = 1/`bench_total' {
                        local is_warmup = (`run' <= `bench_warmup')
                        if "`op'" == "save" {
                            quietly use "`bench_source'", clear
                            timer clear 43
                            timer on 43
                            if "`cmd'" == "pq" quietly pq save using "`pq_file'", replace
                            else quietly dtparquet save "`dtparquet_file'", replace chunksize(`bench_chunk')
                            timer off 43
                            quietly timer list 43
                            local elapsed = r(t43)
                            local out_nobs = _N
                            local out_nvars = c(k)
                        }
                        else {
                            clear
                            timer clear 43
                            timer on 43
                            if "`cmd'" == "pq" quietly pq use using "`pq_file'", clear
                            else quietly dtparquet use using "`dtparquet_file'", clear chunksize(`bench_chunk')
                            timer off 43
                            quietly timer list 43
                            local elapsed = r(t43)
                            local out_nobs = _N
                            local out_nvars = c(k)
                        }
                        post `bench_post' ("`scenario'") ("`cmd'") ("`op'") ///
                            (`run') (`is_warmup') (`elapsed') (`src_nobs') (`src_nvars') ///
                            (`out_nobs') (`out_nvars')
                    }
                }
            }
            capture erase "`dtparquet_file'"
            capture erase "`pq_file'"
        }
        postclose `bench_post'

        preserve
        use "`bench_results'", clear
        keep if warmup == 0
        collapse (mean) mean_elapsed=elapsed (p50) p50_elapsed=elapsed, by(scenario command operation)
        display _newline "BENCH 3 summary (means over measured runs)"
        list scenario command operation mean_elapsed p50_elapsed, noobs
        restore
        local passed_tests "`passed_tests' B3"
        timer off 42
        timer list 42
    }

    // Bench 4: pihps repro + focused comparison (old test9, skips if data missing)
    local pihps_parquet "D:/OneDrive/MyData/pihps/output/pihps.parquet"
    local pihps_dta "D:/OneDrive/MyData/pihps/output/pihps.dta"
    if !fileexists("`pihps_parquet'") | !fileexists("`pihps_dta'") {
        display as text "Bench 4 skipped: pihps data not found under D:/OneDrive/MyData/pihps/output"
    }
    else {
        display _newline "=== BENCH 4: pihps repro + focused comparison ==="
        timer clear 44
        timer on 44
        local ++total_tests
        capture which pq
        local has_pq = (_rc == 0)
        local warmup 1
        local measured 3
        local total = `warmup' + `measured'

        display "Running: dtparquet use `pihps_parquet', clear"
        dtparquet use `pihps_parquet', clear
        display "Running: dtparquet use using `pihps_parquet', clear"
        dtparquet use using `pihps_parquet', clear

        tempname post_repro
        tempfile repro_results
        postfile `post_repro' str10 engine int run byte warmup double seconds ///
            long nobs int nvars using "`repro_results'", replace

        forvalues r = 1/`total' {
            local is_warmup = (`r' <= `warmup')
            clear
            timer clear 45
            timer on 45
            quietly dtparquet use `pihps_parquet', clear timer(plugin) parallel(rows) chunksize(0)
            timer off 45
            quietly timer list 45
            post `post_repro' ("dtparquet") (`r') (`is_warmup') (r(t45)) (_N) (c(k))
            if `has_pq' {
                clear
                timer clear 46
                timer on 46
                quietly pq use `pihps_parquet', clear
                timer off 46
                quietly timer list 46
                post `post_repro' ("pq") (`r') (`is_warmup') (r(t46)) (_N) (c(k))
            }
            clear
            timer clear 47
            timer on 47
            quietly use `pihps_dta', clear
            timer off 47
            quietly timer list 47
            post `post_repro' ("stata") (`r') (`is_warmup') (r(t47)) (_N) (c(k))
        }
        postclose `post_repro'
        preserve
        use "`repro_results'", clear
        keep if warmup == 0
        collapse (mean) mean_seconds=seconds (min) min_seconds=seconds ///
            (max) max_seconds=seconds, by(engine)
        display "BENCH 4 summary over measured runs"
        list engine mean_seconds min_seconds max_seconds, noobs
        restore
        local passed_tests "`passed_tests' B4"
        timer off 44
        timer list 44
    }
}
else {
    display as text "(Bench block skipped; set global DTPARQUET_BENCH 1 to enable.)"
}

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
    exit 0
}
