* Test script for dtparquet Rust plugin

discard
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

* Load the plugin
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll")

* Test 1: Check plugin loads
plugin call dtparquet_plugin, "setup_check"
display as result "Test 1 PASSED: Plugin loaded successfully"

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
program dtparquet_plugin, plugin using("D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll")
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

* Test 6: allstring path for int64->string cast
dtparquet use ID year using "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/bpom_test.parquet", clear allstring
local id_type: type ID
local year_type: type year
assert "`id_type'" == "strL"
assert "`year_type'" == "strL"
display as result "Test 6 PASSED: allstring int64 cast works"

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
capture rmdir "`partition_dir'", all

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

    if ((substr("`typei'", 1, 3) == "str") & ("`typei'" != "strl")) {
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

plugin call dtparquet_plugin, "save" "`filtered_file'" "from_macro" "0" "0" "year > 2015" "from_macros" "" "zstd" "-1" "1" "0" "0"
assert _rc == 0
assert fileexists("`filtered_file'")

dtparquet use using "`filtered_file'", clear
count
assert r(N) > 0
assert year[1] > 2015
summ year, meanonly
assert r(min) > 2015
display as result "Test 9 PASSED: save sql_if filtering works"

* Test 10: Metadata key scaffold is embedded
python:
import pyarrow.parquet as pq
md = pq.read_metadata(r"D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/data/rust_filtered_save.parquet")
assert md.metadata is not None
assert b"dtparquet.dtmeta" in md.metadata
end
display as result "Test 10 PASSED: metadata key scaffold present"

display _newline(2)
display as result "All tests completed!"
display as text "The Rust plugin read and save paths are both validated in batch mode."
