* Test script for dtparquet plugin

version 16
clear frames
discard
capture log close
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
capture log using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/log/dtparquet_test7.log", replace text

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

local failed_tests ""

* Test 1: Check plugin loads
plugin call dtparquet_plugin, "setup_check"
display as result "Test 1 PASSED: Plugin loaded successfully"

* Test 1b: Plugin contract failure paths are deterministic
capture plugin call dtparquet_plugin, "unknown_subfunction"
assert _rc == 198

capture plugin call dtparquet_plugin, "describe"
assert _rc == 198

capture plugin call dtparquet_plugin, "save"
assert _rc == 198

capture plugin call dtparquet_plugin, "read"
assert _rc == 198

capture plugin call dtparquet_plugin, "has_metadata_key"
assert _rc == 198

capture plugin call dtparquet_plugin, "load_meta"
assert _rc == 198
display as result "Test 1b PASSED: plugin subcommand error contracts are deterministic"

* Test 2: Describe contract macros from plugin
plugin call dtparquet_plugin, "describe" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" "1" "0" "" "" "0" "0"
assert real("`n_rows'") > 0
assert real("`n_columns'") > 0
assert "`name_1'" != ""
assert "`type_1'" != ""
assert "`polars_type_1'" != ""
assert real("`string_length_1'") >= 0
display as result "Test 2 PASSED: describe macro contract available"

* Test 3: Call read through ado against real parquet fixture
run "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/dtparquet.ado"
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")
dtparquet use using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/50000, clear
display as result "Test 3 PASSED: Read path executed with ado pre-read setup"
count
assert r(N) == 50000
describe

* Test 4: Subset varlist path
dtparquet use ID PRODUCT_ID fetchdate using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear
assert c(k) == 3
count
assert r(N) > 0
display as result "Test 4 PASSED: varlist subset works"

* Test 5: in-range path
dtparquet use using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/100, clear
count
assert r(N) == 100
display as result "Test 5 PASSED: in-range read works"

* Test 5b: if-qualifier pushdown path
dtparquet use year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" if year > 2015 in 1/2000, clear
count
assert r(N) > 0
assert r(N) <= 2000
summ year, meanonly
assert r(min) > 2015
display as result "Test 5b PASSED: if qualifier filtering is pushed down"

* Test 6: allstring path for int64->string cast
dtparquet use ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear allstring
local id_type: type ID
local year_type: type year
assert "`id_type'" == "strL"
assert "`year_type'" == "strL"
display as result "Test 6 PASSED: allstring int64 cast works"

* Test 6b: foreign categorical compatibility mapping is deterministic
clear
set obs 4
gen str5 cat = ""
replace cat = "red" in 1
replace cat = "blue" in 2
replace cat = "red" in 3
replace cat = "green" in 4
_apply_foreign_cat_labels cat, mode(encode)
local cat_type: type cat
local cat_vallab: value label cat
capture confirm numeric variable cat
assert _rc == 0
assert "`cat_vallab'" == "dtpq_cat_1"
assert cat[1] == 3
assert cat[2] == 1
assert cat[3] == 3
assert cat[4] == 2
tempvar cat_text
decode cat, gen(`cat_text')
assert `cat_text'[1] == "red"
assert `cat_text'[2] == "blue"
assert `cat_text'[3] == "red"
assert `cat_text'[4] == "green"
display as result "Test 6b PASSED: foreign categorical value-label mapping is deterministic"

* Test 6c: catmode(raw) keeps foreign categorical as string
dtparquet use PRODUCT_ID using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/500, clear catmode(raw)
local product_id_type_raw: type PRODUCT_ID
assert substr("`product_id_type_raw'", 1, 3) == "str"
local product_id_vallab_raw: value label PRODUCT_ID
assert "`product_id_vallab_raw'" == ""
display as result "Test 6c PASSED: catmode(raw) preserves string categorical values"

* Test 6d: catmode(both) keeps string and adds deterministic id labels
clear
set obs 4
gen str5 cat = ""
replace cat = "red" in 1
replace cat = "blue" in 2
replace cat = "red" in 3
replace cat = "green" in 4
_apply_foreign_cat_labels cat, mode(both)
capture confirm variable cat_id
assert _rc == 0
capture confirm numeric variable cat_id
assert _rc == 0
local cat_id_vallab: value label cat_id
assert "`cat_id_vallab'" == "dtpq_cat_1"
tempvar cat_id_text
decode cat_id, gen(`cat_id_text')
assert `cat_id_text'[1] == cat[1]
assert `cat_id_text'[2] == cat[2]
assert `cat_id_text'[3] == cat[3]
assert `cat_id_text'[4] == cat[4]
display as result "Test 6d PASSED: catmode(both) adds labeled id companion"

* Test 6e: catmode() validation is deterministic
capture dtparquet use PRODUCT_ID using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear catmode(invalid)
assert _rc == 198
display as result "Test 6e PASSED: catmode invalid value is rejected deterministically"

* Test 6f: fixture-backed pandas categorical in catmode(encode)
local foreign_pandas "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/foreign_cat_pandas.parquet"
dtparquet use cat using "`foreign_pandas'", clear catmode(encode)
count
assert r(N) == 4
capture confirm numeric variable cat
assert _rc == 0
local cat_vallab_foreign: value label cat
assert "`cat_vallab_foreign'" == "dtpq_cat_1"
tempvar cat_foreign_text
decode cat, gen(`cat_foreign_text')
count if missing(`cat_foreign_text')
assert r(N) == 0
display as result "Test 6f PASSED: fixture-backed pandas categorical encode is deterministic"

* Test 6g: fixture-backed dictionary parquet in catmode(raw)
local foreign_arrow "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/foreign_cat_arrow_dict.parquet"
dtparquet use cat using "`foreign_arrow'", clear catmode(raw)
count
assert r(N) == 4
local cat_type_raw_fixture: type cat
assert substr("`cat_type_raw_fixture'", 1, 3) == "str"
local cat_vallab_raw_fixture: value label cat
assert "`cat_vallab_raw_fixture'" == ""
count if missing(cat)
assert r(N) == 0
levelsof cat, local(cat_raw_fixture_levels)
local cat_raw_fixture_level_n : word count `cat_raw_fixture_levels'
assert `cat_raw_fixture_level_n' == 3
display as result "Test 6g PASSED: fixture-backed dictionary raw mode preserves strings"

* Test 6h: fixture-backed dictionary parquet in catmode(both)
dtparquet use cat using "`foreign_arrow'", clear catmode(both)
count
assert r(N) == 4
capture confirm variable cat_id
assert _rc == 0
capture confirm numeric variable cat_id
assert _rc == 0
local cat_id_vallab_fixture: value label cat_id
assert "`cat_id_vallab_fixture'" == "dtpq_cat_1"
tempvar cat_id_fixture_text
decode cat_id, gen(`cat_id_fixture_text')
assert `cat_id_fixture_text'[1] == cat[1]
assert `cat_id_fixture_text'[2] == cat[2]
assert `cat_id_fixture_text'[3] == cat[3]
assert `cat_id_fixture_text'[4] == cat[4]
display as result "Test 6h PASSED: fixture-backed dictionary both mode is deterministic"

* Test 7: Save and read back through dtparquet
local roundtrip_file "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_roundtrip.parquet"
capture erase "`roundtrip_file'"

dtparquet use ID PRODUCT_ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet" in 1/1000, clear
local id_first = ID[1]
local id_last = ID[_N]
local year_first = year[1]

dtparquet save "`roundtrip_file'", replace
assert fileexists("`roundtrip_file'")

dtparquet use using "`roundtrip_file'", clear
count
assert r(N) == 1000
assert c(k) == 3
assert ID[1] == `id_first'
assert ID[_N] == `id_last'
assert year[1] == `year_first'
display as result "Test 7 PASSED: save and read-back roundtrip works"

* Test 8: Plugin save with partition_by + overwrite behavior
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

plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "1" "0" "0"
assert _rc == 0

local partition_dirs : dir "`partition_dir'" dirs "year=*"
local partition_dir_n : word count `partition_dirs'
assert `partition_dir_n' > 0

capture plugin call dtparquet_plugin, "save" "`partition_dir'" "from_macro" "0" "0" "" "from_macros" "year" "zstd" "-1" "0" "0" "0"
assert _rc != 0
display as result "Test 8 PASSED: partition_by save + overwrite guard works"

* Test 9: Plugin save sql_if filter semantics
local filtered_file "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet"
capture erase "`filtered_file'"
capture error 0

plugin call dtparquet_plugin, "save" "`filtered_file'" "from_macro" "0" "0" "year > 2015" "from_macros" "" "zstd" "-1" "1" "0" "0"
local rc_test9_save = _rc
if (`rc_test9_save' != 0) {
    display as error "Test 9 save rc: `rc_test9_save'"
}
assert `rc_test9_save' == 0
assert fileexists("`filtered_file'")

dtparquet use using "`filtered_file'", clear
count
assert r(N) > 0
assert year[1] > 2015
summ year, meanonly
assert r(min) > 2015
display as result "Test 9 PASSED: save sql_if filtering works"

* Test 9b: Plugin save compression accepted values + default fallback
local compress_zstd "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_zstd.parquet"
local compress_uncompressed "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_uncompressed.parquet"
local compress_default "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_default.parquet"
local compress_bad "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_bad.parquet"
local compress_level_bad "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_compress_level_bad.parquet"

capture erase "`compress_zstd'"
capture erase "`compress_uncompressed'"
capture erase "`compress_default'"
capture erase "`compress_bad'"
capture erase "`compress_level_bad'"

dtparquet save "`compress_zstd'", replace compress(zstd)
assert _rc == 0
assert fileexists("`compress_zstd'")

dtparquet save "`compress_uncompressed'", replace compress(uncompressed)
assert _rc == 0
assert fileexists("`compress_uncompressed'")

dtparquet save "`compress_default'", replace
assert _rc == 0
assert fileexists("`compress_default'")

capture dtparquet save "`compress_bad'", replace compress(invalid_codec)
assert _rc == 198

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "zstd" "3" "1" "0" "0"
assert _rc == 198

capture plugin call dtparquet_plugin, "save" "`compress_level_bad'" "from_macro" "0" "0" "" "from_macros" "" "snappy" "1" "1" "0" "0"
assert _rc == 198

dtparquet use using "`compress_zstd'", clear
count
assert r(N) > 0

dtparquet use using "`compress_uncompressed'", clear
count
assert r(N) > 0

dtparquet use using "`compress_default'", clear
count
assert r(N) > 0
display as result "Test 9b PASSED: compress() codec/default and level semantics are deterministic"

* Test 9c: compress_string_to_numeric remains unsupported
capture dtparquet save "`compress_bad'", replace compress_string_to_numeric
assert _rc == 198
display as result "Test 9c PASSED: compress_string_to_numeric is deterministically unsupported"

* Test 10: Metadata key scaffold is embedded
plugin call dtparquet_plugin, "has_metadata_key" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet" "dtparquet.dtmeta"
assert "`has_metadata_key'" == "1"
display as result "Test 10 PASSED: metadata key scaffold present"

* Test 11: dtparquet export/import command paths
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

dtparquet export "`export_parquet'" using "`source_dta'"
confirm file "`export_parquet'"

capture dtparquet export "`export_parquet'" using "`source_dta'"
assert _rc != 0

dtparquet import "`import_default_dta'" using "`export_parquet'"
quietly use "`import_default_dta'", clear
count
assert r(N) == 3
assert c(k) == 4
assert id[1] == 1
assert code[3] == 1234567890123

dtparquet export "`export_parquet'" using "`source_dta'", replace nolabel
dtparquet import "`import_allstring_dta'" using "`export_parquet'", replace nolabel
quietly use "`import_allstring_dta'", clear
count
assert r(N) == 3
assert c(k) == 4
assert id[1] == 1
assert code[3] == 1234567890123

dtparquet import "`import_allstring_dta'" using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", replace allstring
quietly use "`import_allstring_dta'", clear
local id_type: type ID
local year_type: type year
assert "`id_type'" == "strL"
assert "`year_type'" == "strL"
display as result "Test 11 PASSED: export/import supports replace nolabel allstring and quoted paths"

* Test 12: parser edge-case failures for export/import
capture dtparquet export "`export_parquet'" "`source_dta'"
assert _rc == 198

capture dtparquet export using "`source_dta'"
assert _rc == 198

capture dtparquet export "`export_parquet'" using
assert _rc == 198

capture dtparquet import "`import_default_dta'" "`export_parquet'"
assert _rc == 198

capture dtparquet import using "`export_parquet'"
assert _rc == 198

capture dtparquet import "`import_default_dta'" using
assert _rc == 198
display as result "Test 12 PASSED: export/import parser failure paths are stable"

* Test 13: metadata roundtrip semantics
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

dtparquet save "`meta_parquet'", replace
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
assert "`dtmeta_loaded'" == "1"
assert real("`dtmeta_dta_obs'") == 3
assert real("`dtmeta_dta_vars'") == 1
assert "`dtmeta_dta_ts'" != ""
assert real("`dtmeta_dta_note_count'") == 1
assert real("`dtmeta_var_note_count'") == 1
dtparquet use using "`meta_parquet'", clear
local z_var_label_default : variable label z
local z_val_label_default : value label z
local d_label_default : data label
notes _count d_note_count_default : _dta
notes _count z_note_count_default : z
assert "`z_var_label_default'" == "z label"
assert "`z_val_label_default'" == "zlbl"
assert "`d_label_default'" == "meta roundtrip"
assert `d_note_count_default' == 1
assert `z_note_count_default' == 1

dtparquet save "`meta_parquet'", replace nolabel
plugin call dtparquet_plugin, "load_meta" "`meta_parquet'"
assert "`dtmeta_loaded'" == "1"
assert real("`dtmeta_dta_obs'") == 0
assert real("`dtmeta_dta_vars'") == 0
assert "`dtmeta_dta_ts'" == ""
assert real("`dtmeta_dta_note_count'") == 0
assert real("`dtmeta_var_note_count'") == 0
dtparquet use using "`meta_parquet'", clear
local z_var_label_nolabel : variable label z
local z_val_label_nolabel : value label z
local d_label_nolabel : data label
notes _count d_note_count_nolabel : _dta
notes _count z_note_count_nolabel : z
assert "`z_var_label_nolabel'" == ""
assert "`z_val_label_nolabel'" == ""
assert "`d_label_nolabel'" == ""
assert `d_note_count_nolabel' == 0
assert `z_note_count_nolabel' == 0
display as result "Test 13 PASSED: metadata behavior is deterministic with and without nolabel"

* Test 14: partitioned metadata embedding
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

plugin call dtparquet_plugin, "load_meta" "`leaf_path'"
assert "`dtmeta_loaded'" == "1"
assert "`dtmeta_dta_vars'" == "2" 
dtparquet use using "`leaf_path'", clear
local z_var_label : variable label z
assert "`z_var_label'" == "z label"
display as result "Test 14 PASSED: partitioned metadata embedding works"
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

display _newline(2)
display "=========================================="
display "Test Suite Summary"
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
    capture erase "dtparquet_test7.log"
    exit 0
}
