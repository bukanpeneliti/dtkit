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

* Test 7: Call save subcommand (placeholder)
plugin call dtparquet_plugin, "save" "test.parquet" "var1 var2" "100" "0" "" "{}" "" "zstd" "-1" "1" "0" "0"
display as result "Test 7 COMPLETED: Save placeholder called"

display _newline(2)
display as result "All tests completed!"
display as text "The Rust plugin is working. Next: implement actual read/write logic."
