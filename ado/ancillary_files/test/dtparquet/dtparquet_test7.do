* Test script for dtparquet plugin

version 16
clear frames
discard
capture log close
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
log using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/log/dtparquet_test7.log", replace text

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

* Deterministic pre-clean for generated outputs from prior interrupted runs
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_bad_protocol.parquet"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet.tmp"
capture noisily _cleanup_dir_shallow "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_partitioned_out"

* Load the plugin
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
capture noisily copy "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
local promote_rc = _rc
if _rc != 0 {
    local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll"
}

if `promote_rc' == 0 {
    assert "`plugin_dll'" == "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
}
else {
    assert "`plugin_dll'" == "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll"
}

cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Initialize test tracking
local passed_tests ""
local failed_tests ""
local total_tests 0

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet Plugin Test Suite"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case 1: Check plugin loads
display _newline "=== TEST CASE 1: Plugin loads ==="
local ++total_tests
plugin call dtparquet_plugin, "setup_check"
if _rc == 0 {
    display as result "Test 1 completed successfully"
    local passed_tests "`passed_tests' 1"
}
else {
    display as error "Test 1 failed: plugin did not load"
    local failed_tests "`failed_tests' 1"
}

// Test Case 1b: Plugin contract failure paths are deterministic
display _newline "=== TEST CASE 1b: Plugin contract failure paths ==="
local ++total_tests
local t1b_err 0
capture plugin call dtparquet_plugin, "unknown_subfunction"
if _rc != 198 local ++t1b_err

capture plugin call dtparquet_plugin, "describe"
if _rc != 198 local ++t1b_err

capture plugin call dtparquet_plugin, "save"
if _rc != 198 local ++t1b_err

capture plugin call dtparquet_plugin, "read"
if _rc != 198 local ++t1b_err

capture plugin call dtparquet_plugin, "has_metadata_key"
if _rc != 198 local ++t1b_err

capture plugin call dtparquet_plugin, "load_meta"
if _rc != 198 local ++t1b_err

if `t1b_err' == 0 {
    display as result "Test 1b completed successfully"
    local passed_tests "`passed_tests' 1b"
}
else {
    display as error "Test 1b failed: plugin contract failure paths not deterministic"
    local failed_tests "`failed_tests' 1b"
}

// Test Case 2: Describe contract macros from plugin
display _newline "=== TEST CASE 2: Describe contract macros ==="
local ++total_tests
plugin call dtparquet_plugin, "describe" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" "1" "0" "" "" "0" "0"
local t2_err 0
if real("`n_rows'") <= 0 local ++t2_err
if real("`n_columns'") <= 0 local ++t2_err
if "`name_1'" == "" local ++t2_err
if "`type_1'" == "" local ++t2_err
if "`polars_type_1'" == "" local ++t2_err
if real("`string_length_1'") < 0 local ++t2_err
if real("`schema_protocol_version'") != 2 local ++t2_err
if `"`schema_payload'"' == "" local ++t2_err

if `t2_err' == 0 {
    display as result "Test 2 completed successfully"
    local passed_tests "`passed_tests' 2"
}
else {
    display as error "Test 2 failed: describe macro contract not available"
    local failed_tests "`failed_tests' 2"
}

// Test Case 2b: schema protocol mismatch is explicit
display _newline "=== TEST CASE 2b: Schema protocol mismatch ==="
local ++total_tests
clear
set obs 2
gen long id = _n
local bad_schema_payload `"{""protocol_version"":999,""fields"":[{""name"":""id"",""dtype"":""long"",""format"":""%12.0g"",""str_length"":0}]}"'
capture plugin call dtparquet_plugin, "save" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_bad_protocol.parquet" "id" "0" "0" "" "`bad_schema_payload'" "" "zstd" "-1" "1" "0" "1" "1000"
local t2b_err 0
if _rc != 198 local ++t2b_err
if fileexists("D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_bad_protocol.parquet") != 0 local ++t2b_err

if `t2b_err' == 0 {
    display as result "Test 2b completed successfully"
    local passed_tests "`passed_tests' 2b"
}
else {
    display as error "Test 2b failed: protocol mismatch did not fail fast"
    local failed_tests "`failed_tests' 2b"
}

// Test Case 3: Call read through ado against real parquet fixture
display _newline "=== TEST CASE 3: Read path with ado pre-read setup ==="
local ++total_tests
capture program drop dtparquet
run "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/dtparquet.ado"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")
dtparquet use using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/50000, clear
local t3_err 0
if inlist("$read_schema_handoff", "json_v2", "legacy_macros") == 0 local ++t3_err
count
if r(N) != 50000 local ++t3_err

if `t3_err' == 0 {
    display as result "Test 3 completed successfully"
    local passed_tests "`passed_tests' 3"
}
else {
    display as error "Test 3 failed: read path did not execute correctly"
    local failed_tests "`failed_tests' 3"
}

// Test Case 4: Subset varlist path
display _newline "=== TEST CASE 4: Subset varlist path ==="
local ++total_tests
dtparquet use ID PRODUCT_ID fetchdate using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear
local t4_err 0
if c(k) != 3 local ++t4_err
count
if r(N) <= 0 local ++t4_err

if `t4_err' == 0 {
    display as result "Test 4 completed successfully"
    local passed_tests "`passed_tests' 4"
}
else {
    display as error "Test 4 failed: varlist subset did not work"
    local failed_tests "`failed_tests' 4"
}

// Test Case 5: in-range path
display _newline "=== TEST CASE 5: In-range read ==="
local ++total_tests
dtparquet use using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/100, clear
count
if r(N) == 100 {
    display as result "Test 5 completed successfully"
    local passed_tests "`passed_tests' 5"
}
else {
    display as error "Test 5 failed: in-range read did not work"
    local failed_tests "`failed_tests' 5"
}

// Test Case 5b: if-qualifier pushdown path
display _newline "=== TEST CASE 5b: If-qualifier pushdown ==="
local ++total_tests
dtparquet use year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" if year > 2015 in 1/2000, clear
local t5b_err 0
count
if r(N) <= 0 local ++t5b_err
if r(N) > 2000 local ++t5b_err
summ year, meanonly
if r(min) <= 2015 local ++t5b_err
if "$if_filter_mode" != "expr" local ++t5b_err

if `t5b_err' == 0 {
    display as result "Test 5b completed successfully"
    local passed_tests "`passed_tests' 5b"
}
else {
    display as error "Test 5b failed: if-qualifier pushdown did not work"
    local failed_tests "`failed_tests' 5b"
}

// Test Case 6: allstring path for int64->string cast
display _newline "=== TEST CASE 6: Allstring int64 cast ==="
local ++total_tests
dtparquet use ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear allstring
local id_type: type ID
local year_type: type year
if "`id_type'" == "strL" & "`year_type'" == "strL" {
    display as result "Test 6 completed successfully"
    local passed_tests "`passed_tests' 6"
}
else {
    display as error "Test 6 failed: allstring int64 cast did not work"
    local failed_tests "`failed_tests' 6"
}

// Test Case 6b: foreign categorical compatibility mapping is deterministic
display _newline "=== TEST CASE 6b: Foreign categorical mapping ==="
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
local t6b_err 0
capture confirm numeric variable cat
if _rc != 0 local ++t6b_err
if "`cat_vallab'" != "cat_1" local ++t6b_err
if cat[1] != 3 local ++t6b_err
if cat[2] != 1 local ++t6b_err
if cat[3] != 3 local ++t6b_err
if cat[4] != 2 local ++t6b_err
tempvar cat_text
decode cat, gen(`cat_text')
if `cat_text'[1] != "red" local ++t6b_err
if `cat_text'[2] != "blue" local ++t6b_err
if `cat_text'[3] != "red" local ++t6b_err
if `cat_text'[4] != "green" local ++t6b_err

if `t6b_err' == 0 {
    display as result "Test 6b completed successfully"
    local passed_tests "`passed_tests' 6b"
}
else {
    display as error "Test 6b failed: foreign categorical mapping not deterministic"
    local failed_tests "`failed_tests' 6b"
}

// Test Case 6c: catmode(raw) keeps foreign categorical as string
display _newline "=== TEST CASE 6c: Catmode(raw) preserves strings ==="
local ++total_tests
dtparquet use PRODUCT_ID using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/500, clear catmode(raw)
local product_id_type_raw: type PRODUCT_ID
local product_id_vallab_raw: value label PRODUCT_ID
if substr("`product_id_type_raw'", 1, 3) == "str" & "`product_id_vallab_raw'" == "" {
    display as result "Test 6c completed successfully"
    local passed_tests "`passed_tests' 6c"
}
else {
    display as error "Test 6c failed: catmode(raw) did not preserve strings"
    local failed_tests "`failed_tests' 6c"
}

// Test Case 6d: catmode(both) keeps string and adds deterministic id labels
display _newline "=== TEST CASE 6d: Catmode(both) adds labeled id ==="
local ++total_tests
clear
set obs 4
gen str5 cat = ""
replace cat = "red" in 1
replace cat = "blue" in 2
replace cat = "red" in 3
replace cat = "green" in 4
_apply_foreign_cat_labels cat, mode(both)
local cat_id_vallab: value label cat_id
local t6d_err 0
capture confirm variable cat_id
if _rc != 0 local ++t6d_err
capture confirm numeric variable cat_id
if _rc != 0 local ++t6d_err
if "`cat_id_vallab'" != "cat_1" local ++t6d_err
tempvar cat_id_text
decode cat_id, gen(`cat_id_text')
if `cat_id_text'[1] != cat[1] local ++t6d_err
if `cat_id_text'[2] != cat[2] local ++t6d_err
if `cat_id_text'[3] != cat[3] local ++t6d_err
if `cat_id_text'[4] != cat[4] local ++t6d_err

if `t6d_err' == 0 {
    display as result "Test 6d completed successfully"
    local passed_tests "`passed_tests' 6d"
}
else {
    display as error "Test 6d failed: catmode(both) did not add labeled id"
    local failed_tests "`failed_tests' 6d"
}

// Test Case 6e: catmode() validation is deterministic
display _newline "=== TEST CASE 6e: Catmode validation ==="
local ++total_tests
capture dtparquet use PRODUCT_ID using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear catmode(invalid)
if _rc == 198 {
    display as result "Test 6e completed successfully"
    local passed_tests "`passed_tests' 6e"
}
else {
    display as error "Test 6e failed: catmode invalid value not rejected"
    local failed_tests "`failed_tests' 6e"
}

// Test Case 6f: fixture-backed pandas categorical in catmode(encode)
display _newline "=== TEST CASE 6f: Pandas categorical encode ==="
local ++total_tests
local foreign_pandas "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/foreign_cat_pandas.parquet"
dtparquet use cat using "`foreign_pandas'", clear catmode(encode)
local t6f_err 0
count
if r(N) != 4 local ++t6f_err
capture confirm numeric variable cat
if _rc != 0 local ++t6f_err
local cat_vallab_foreign: value label cat
if "`cat_vallab_foreign'" != "cat_1" local ++t6f_err
tempvar cat_foreign_text
decode cat, gen(`cat_foreign_text')
count if missing(`cat_foreign_text')
if r(N) != 0 local ++t6f_err

if `t6f_err' == 0 {
    display as result "Test 6f completed successfully"
    local passed_tests "`passed_tests' 6f"
}
else {
    display as error "Test 6f failed: pandas categorical encode not deterministic"
    local failed_tests "`failed_tests' 6f"
}

// Test Case 6g: fixture-backed dictionary parquet in catmode(raw)
display _newline "=== TEST CASE 6g: Dictionary raw mode ==="
local ++total_tests
local foreign_arrow "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/foreign_cat_arrow_dict.parquet"
dtparquet use cat using "`foreign_arrow'", clear catmode(raw)
local cat_type_raw_fixture: type cat
local cat_vallab_raw_fixture: value label cat
local t6g_err 0
count
if r(N) != 4 local ++t6g_err
if substr("`cat_type_raw_fixture'", 1, 3) != "str" local ++t6g_err
if "`cat_vallab_raw_fixture'" != "" local ++t6g_err
count if missing(cat)
if r(N) != 0 local ++t6g_err
levelsof cat, local(cat_raw_fixture_levels)
local cat_raw_fixture_level_n : word count `cat_raw_fixture_levels'
if `cat_raw_fixture_level_n' != 3 local ++t6g_err

if `t6g_err' == 0 {
    display as result "Test 6g completed successfully"
    local passed_tests "`passed_tests' 6g"
}
else {
    display as error "Test 6g failed: dictionary raw mode did not preserve strings"
    local failed_tests "`failed_tests' 6g"
}

// Test Case 6h: fixture-backed dictionary parquet in catmode(both)
display _newline "=== TEST CASE 6h: Dictionary both mode ==="
local ++total_tests
dtparquet use cat using "`foreign_arrow'", clear catmode(both)
local cat_id_vallab_fixture: value label cat_id
local t6h_err 0
count
if r(N) != 4 local ++t6h_err
capture confirm variable cat_id
if _rc != 0 local ++t6h_err
capture confirm numeric variable cat_id
if _rc != 0 local ++t6h_err
if "`cat_id_vallab_fixture'" != "cat_1" local ++t6h_err
tempvar cat_id_fixture_text
decode cat_id, gen(`cat_id_fixture_text')
if `cat_id_fixture_text'[1] != cat[1] local ++t6h_err
if `cat_id_fixture_text'[2] != cat[2] local ++t6h_err
if `cat_id_fixture_text'[3] != cat[3] local ++t6h_err
if `cat_id_fixture_text'[4] != cat[4] local ++t6h_err

if `t6h_err' == 0 {
    display as result "Test 6h completed successfully"
    local passed_tests "`passed_tests' 6h"
}
else {
    display as error "Test 6h failed: dictionary both mode not deterministic"
    local failed_tests "`failed_tests' 6h"
}

// Test Case 7: Save and read back through dtparquet
display _newline "=== TEST CASE 7: Save and read-back roundtrip ==="
local ++total_tests
local roundtrip_file "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet"
capture erase "`roundtrip_file'"

dtparquet use ID PRODUCT_ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/1000, clear
local id_first = ID[1]
local id_last = ID[_N]
local year_first = year[1]

dtparquet save "`roundtrip_file'", replace
local t7_err 0
if fileexists("`roundtrip_file'") == 0 local ++t7_err
if inlist("$write_schema_handoff", "json_v2", "legacy_macros") == 0 local ++t7_err

dtparquet use using "`roundtrip_file'", clear
count
if r(N) != 1000 local ++t7_err
if c(k) != 3 local ++t7_err
if ID[1] != `id_first' local ++t7_err
if ID[_N] != `id_last' local ++t7_err
if year[1] != `year_first' local ++t7_err

if `t7_err' == 0 {
    display as result "Test 7 completed successfully"
    local passed_tests "`passed_tests' 7"
}
else {
    display as error "Test 7 failed: save and read-back roundtrip did not work"
    local failed_tests "`failed_tests' 7"
}

// Test Case 8: Plugin save with partition_by + overwrite behavior
display _newline "=== TEST CASE 8: Partition_by save + overwrite guard ==="
local ++total_tests
local partition_dir "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_partitioned_out"
capture noisily _cleanup_dir_shallow "`partition_dir'"
capture error 0

dtparquet use ID PRODUCT_ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/1000, clear
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

local t8_err 0
plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "1" "0" "0"
if _rc != 0 local ++t8_err

local partition_dirs : dir "`partition_dir'" dirs "year=*"
local partition_dir_n : word count `partition_dirs'
if `partition_dir_n' <= 0 local ++t8_err

capture plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "0" "0" "0"
if _rc == 0 local ++t8_err

if `t8_err' == 0 {
    display as result "Test 8 completed successfully"
    local passed_tests "`passed_tests' 8"
}
else {
    display as error "Test 8 failed: partition_by save + overwrite guard did not work"
    local failed_tests "`failed_tests' 8"
}

// Test Case 9: Plugin save sql_if filter semantics
display _newline "=== TEST CASE 9: Save sql_if filtering ==="
local ++total_tests
local filtered_file "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet"
capture erase "`filtered_file'"
capture error 0

plugin call dtparquet_plugin, "save" "`filtered_file'" "from_macro" "0" "0" "year > 2015" "from_macros" "" "zstd" "-1" "1" "0" "0"
local rc_test9_save = _rc
local t9_err 0
if (`rc_test9_save' != 0) local ++t9_err
if fileexists("`filtered_file'") == 0 local ++t9_err

dtparquet use using "`filtered_file'", clear
count
if r(N) <= 0 local ++t9_err
if year[1] <= 2015 local ++t9_err
summ year, meanonly
if r(min) <= 2015 local ++t9_err

if `t9_err' == 0 {
    display as result "Test 9 completed successfully"
    local passed_tests "`passed_tests' 9"
}
else {
    display as error "Test 9 failed: save sql_if filtering did not work"
    local failed_tests "`failed_tests' 9"
}

// Test Case 9b: Plugin save compression accepted values + default fallback
display _newline "=== TEST CASE 9b: Compression codec/default semantics ==="
local ++total_tests
local compress_zstd "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_zstd.parquet"
local compress_uncompressed "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_uncompressed.parquet"
local compress_default "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_default.parquet"
local compress_level_bad "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_level_bad.parquet"

capture erase "`compress_zstd'"
capture erase "`compress_uncompressed'"
capture erase "`compress_default'"
capture erase "`compress_level_bad'"

local t9b_err 0
dtparquet save "`compress_zstd'", replace compress(zstd)
if _rc != 0 local ++t9b_err
if fileexists("`compress_zstd'") == 0 local ++t9b_err

dtparquet save "`compress_uncompressed'", replace compress(uncompressed)
if _rc != 0 local ++t9b_err
if fileexists("`compress_uncompressed'") == 0 local ++t9b_err

dtparquet save "`compress_default'", replace
if _rc != 0 local ++t9b_err
if fileexists("`compress_default'") == 0 local ++t9b_err

capture dtparquet save "`compress_bad'", replace compress(invalid_codec)
if _rc != 198 local ++t9b_err

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "zstd" "3" "1" "0" "0"
if _rc != 198 local ++t9b_err

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "snappy" "1" "1" "0" "0"
if _rc != 198 local ++t9b_err

dtparquet use using "`compress_zstd'", clear
count
if r(N) <= 0 local ++t9b_err

dtparquet use using "`compress_uncompressed'", clear
count
if r(N) <= 0 local ++t9b_err

dtparquet use using "`compress_default'", clear
count
if r(N) <= 0 local ++t9b_err

if `t9b_err' == 0 {
    display as result "Test 9b completed successfully"
    local passed_tests "`passed_tests' 9b"
}
else {
    display as error "Test 9b failed: compress() codec/default semantics not deterministic"
    local failed_tests "`failed_tests' 9b"
}

// Test Case 9c: compress_string_to_numeric remains unsupported
display _newline "=== TEST CASE 9c: Compress_string_to_numeric unsupported ==="
local ++total_tests
capture dtparquet save "`compress_bad'", replace compress_string_to_numeric
if _rc == 198 {
    display as result "Test 9c completed successfully"
    local passed_tests "`passed_tests' 9c"
}
else {
    display as error "Test 9c failed: compress_string_to_numeric not rejected"
    local failed_tests "`failed_tests' 9c"
}

// Test Case 10: Metadata key scaffold is embedded
display _newline "=== TEST CASE 10: Metadata key scaffold ==="
local ++total_tests
plugin call dtparquet_plugin, "has_metadata_key" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet" "dtparquet.dtmeta"
if "`has_metadata_key'" == "1" {
    display as result "Test 10 completed successfully"
    local passed_tests "`passed_tests' 10"
}
else {
    display as error "Test 10 failed: metadata key scaffold not present"
    local failed_tests "`failed_tests' 10"
}

// Test Case 11: dtparquet export/import command paths
display _newline "=== TEST CASE 11: Export/import command paths ==="
local ++total_tests
local export_dir "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/export import tmp"
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

local t11_err 0
dtparquet export "`export_parquet'" using "`source_dta'"
if fileexists("`export_parquet'") == 0 local ++t11_err

capture dtparquet export "`export_parquet'" using "`source_dta'"
if _rc == 0 local ++t11_err

dtparquet import "`import_default_dta'" using "`export_parquet'"
quietly use "`import_default_dta'", clear
count
if r(N) != 3 local ++t11_err
if c(k) != 4 local ++t11_err
if id[1] != 1 local ++t11_err
if code[3] != 1234567890123 local ++t11_err

dtparquet export "`export_parquet'" using "`source_dta'", replace nolabel
dtparquet import "`import_allstring_dta'" using "`export_parquet'", replace nolabel
quietly use "`import_allstring_dta'", clear
count
if r(N) != 3 local ++t11_err
if c(k) != 4 local ++t11_err
if id[1] != 1 local ++t11_err
if code[3] != 1234567890123 local ++t11_err

dtparquet import "`import_allstring_dta'" using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", replace allstring
quietly use "`import_allstring_dta'", clear
local id_type: type ID
local year_type: type year
if "`id_type'" != "strL" local ++t11_err
if "`year_type'" != "strL" local ++t11_err

if `t11_err' == 0 {
    display as result "Test 11 completed successfully"
    local passed_tests "`passed_tests' 11"
}
else {
    display as error "Test 11 failed: export/import command paths did not work"
    local failed_tests "`failed_tests' 11"
}

// Test Case 12: parser edge-case failures for export/import
display _newline "=== TEST CASE 12: Export/import parser failures ==="
local ++total_tests
local t12_err 0
capture dtparquet export "`export_parquet'" "`source_dta'"
if _rc != 198 local ++t12_err

capture dtparquet export using "`source_dta'"
if _rc != 198 local ++t12_err

capture dtparquet export "`export_parquet'" using
if _rc != 198 local ++t12_err

capture dtparquet import "`import_default_dta'" "`export_parquet'"
if _rc != 198 local ++t12_err

capture dtparquet import using "`export_parquet'"
if _rc != 198 local ++t12_err

capture dtparquet import "`import_default_dta'" using
if _rc != 198 local ++t12_err

if `t12_err' == 0 {
    display as result "Test 12 completed successfully"
    local passed_tests "`passed_tests' 12"
}
else {
    display as error "Test 12 failed: export/import parser failures not stable"
    local failed_tests "`failed_tests' 12"
}

// Test Case 13: metadata roundtrip semantics
display _newline "=== TEST CASE 13: Metadata roundtrip ==="
local ++total_tests
local meta_parquet "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/meta_roundtrip.parquet"
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

local t13_err 0
dtparquet save "`meta_parquet'", replace
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
if "`dtmeta_loaded'" != "1" local ++t13_err
if real("`dtmeta_dta_obs'") != 3 local ++t13_err
if real("`dtmeta_dta_vars'") != 1 local ++t13_err
if "`dtmeta_dta_ts'" == "" local ++t13_err
if real("`dtmeta_dta_note_count'") != 1 local ++t13_err
if real("`dtmeta_var_note_count'") != 1 local ++t13_err

dtparquet use using "`meta_parquet'", clear
local z_var_label_default : variable label z
local z_val_label_default : value label z
local d_label_default : data label
notes _count d_note_count_default : _dta
notes _count z_note_count_default : z
if "`z_var_label_default'" != "z label" local ++t13_err
if "`z_val_label_default'" != "zlbl" local ++t13_err
if "`d_label_default'" != "meta roundtrip" local ++t13_err
if `d_note_count_default' != 1 local ++t13_err
if `z_note_count_default' != 1 local ++t13_err

dtparquet save "`meta_parquet'", replace nolabel
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
if "`dtmeta_loaded'" != "1" local ++t13_err
if real("`dtmeta_dta_obs'") != 0 local ++t13_err
if real("`dtmeta_dta_vars'") != 0 local ++t13_err
if "`dtmeta_dta_ts'" != "" local ++t13_err
if real("`dtmeta_dta_note_count'") != 0 local ++t13_err
if real("`dtmeta_var_note_count'") != 0 local ++t13_err

dtparquet use using "`meta_parquet'", clear
local z_var_label_nolabel : variable label z
local z_val_label_nolabel : value label z
local d_label_nolabel : data label
notes _count d_note_count_nolabel : _dta
notes _count z_note_count_nolabel : z
if "`z_var_label_nolabel'" != "" local ++t13_err
if "`z_val_label_nolabel'" != "" local ++t13_err
if "`d_label_nolabel'" != "" local ++t13_err
if `d_note_count_nolabel' != 0 local ++t13_err
if `z_note_count_nolabel' != 0 local ++t13_err

if `t13_err' == 0 {
    display as result "Test 13 completed successfully"
    local passed_tests "`passed_tests' 13"
}
else {
    display as error "Test 13 failed: metadata behavior not deterministic"
    local failed_tests "`failed_tests' 13"
}

// Test Case 14: partitioned metadata embedding
display _newline "=== TEST CASE 14: Partitioned metadata embedding ==="
local ++total_tests
local partition_meta_dir "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/partition_meta"
capture noisily _cleanup_dir_shallow "`partition_meta_dir'"

clear
set obs 10
gen byte group = (_n > 5)
gen byte z = _n
label define zlbl 1 "one" 2 "two" 3 "three", replace
label values z zlbl
label variable z "z label"
label data "partitioned meta test"

dtparquet save "`partition_meta_dir'", partition_by(group) replace

* Check metadata in one of the leaf files
local leaf_file "`partition_meta_dir'/group=0"
local leaf_files : dir "`leaf_file'" files "*.parquet"
local first_leaf : word 1 of `leaf_files'
local leaf_path "`leaf_file'/`first_leaf'"

local t14_err 0
plugin call dtparquet_plugin, "load_meta" "`leaf_path'"
if "`dtmeta_loaded'" != "1" local ++t14_err
if "`dtmeta_dta_vars'" != "2" local ++t14_err
dtparquet use using "`leaf_path'", clear
local z_var_label : variable label z
if "`z_var_label'" != "z label" local ++t14_err

if `t14_err' == 0 {
    display as result "Test 14 completed successfully"
    local passed_tests "`passed_tests' 14"
}
else {
    display as error "Test 14 failed: partitioned metadata embedding did not work"
    local failed_tests "`failed_tests' 14"
}
capture noisily _cleanup_dir_shallow "`partition_meta_dir'"

capture erase "`meta_parquet'"
capture erase "`roundtrip_file'"
capture erase "`filtered_file'"
capture erase "`compress_zstd'"
capture erase "`compress_uncompressed'"
capture erase "`compress_default'"
capture erase "`compress_bad'"
capture erase "`compress_level_bad'"
capture erase "`roundtrip_file'.tmp"
capture erase "`filtered_file'.tmp"
capture erase "`compress_zstd'.tmp"
capture erase "`compress_uncompressed'.tmp"
capture erase "`compress_default'.tmp"
capture erase "`compress_bad'.tmp"
capture erase "`compress_level_bad'.tmp"
capture noisily _cleanup_dir_shallow "`partition_dir'"

capture erase "`source_dta'"
capture erase "`export_parquet'"
capture erase "`import_default_dta'"
capture erase "`import_allstring_dta'"
capture noisily _cleanup_dir_shallow "`export_dir'"

* Native deterministic cleanup for tmp artifacts
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_zstd.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_uncompressed.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_default.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_bad.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_level_bad.parquet.tmp"

// Cleanup
capture erase "`meta_parquet'"
capture erase "`roundtrip_file'"
capture erase "`filtered_file'"
capture erase "`compress_zstd'"
capture erase "`compress_uncompressed'"
capture erase "`compress_default'"
capture erase "`compress_bad'"
capture erase "`compress_level_bad'"
capture erase "`roundtrip_file'.tmp"
capture erase "`filtered_file'.tmp"
capture erase "`compress_zstd'.tmp"
capture erase "`compress_uncompressed'.tmp"
capture erase "`compress_default'.tmp"
capture erase "`compress_bad'.tmp"
capture erase "`compress_level_bad'.tmp"
capture noisily _cleanup_dir_shallow "`partition_dir'"

capture erase "`source_dta'"
capture erase "`export_parquet'"
capture erase "`import_default_dta'"
capture erase "`import_allstring_dta'"
capture noisily _cleanup_dir_shallow "`export_dir'"

* Native deterministic cleanup for tmp artifacts
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_zstd.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_uncompressed.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_default.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_bad.parquet.tmp"
capture erase "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_level_bad.parquet.tmp"

// Test Summary
display _newline(2) "=========================================="
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
