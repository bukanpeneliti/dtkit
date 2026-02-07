* Test script for dtparquet Rust plugin

discard

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
run "ado/dtparquet.ado"
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

display _newline(2)
display as result "All tests completed!"
display as text "The Rust plugin read and save paths are both validated in batch mode."
