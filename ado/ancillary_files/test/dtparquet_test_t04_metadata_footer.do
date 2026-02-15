*! dtparquet_test_t04_metadata_footer.do

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
adopath ++ "ado"
run "ado/dtparquet.ado"

local log_file "ado/ancillary_files/test/log/dtparquet_test_t04_metadata_footer.log"
log using "`log_file'", text replace

tempfile with_meta_stub

set obs 30
gen long id = _n
gen str20 tag = "row_" + string(_n, "%03.0f")

dtparquet save "`with_meta_stub'", replace

local with_meta_file "`with_meta_stub'.parquet"
plugin call dtparquet_plugin, "has_metadata_key" "`with_meta_file'" "dtparquet.dtmeta"
assert "`has_metadata_key'" == "1"

plugin call dtparquet_plugin, "has_metadata_key" "`with_meta_file'" "does.not.exist"
assert "`has_metadata_key'" == "0"

local fixture_no_meta "ado/ancillary_files/test/dtparquet/data/bpom_test.parquet"
plugin call dtparquet_plugin, "has_metadata_key" "`fixture_no_meta'" "dtparquet.dtmeta"
assert "`has_metadata_key'" == "0"

display as result "T04 metadata footer lookup check passed"

log close
cd "`initial_pwd'"
