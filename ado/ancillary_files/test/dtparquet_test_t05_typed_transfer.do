*! dtparquet_test_t05_typed_transfer.do

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
adopath ++ "ado"
run "ado/dtparquet.ado"

local log_file "ado/ancillary_files/test/log/dtparquet_test_t05_typed_transfer.log"
log using "`log_file'", text replace

local n_rows 10000
local chunk_size 2500

tempfile parquet_stub

set obs `n_rows'
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

dtparquet save "`parquet_stub'", replace chunksize(`chunk_size')

local parquet_file "`parquet_stub'.parquet"

clear
dtparquet use using "`parquet_file'", clear chunksize(`chunk_size')

assert _N == `n_rows'
assert c(k) == 9

local fallback = real("$dtpq_read_fallback_calls")
local conversion_failures = real("$dtpq_read_conversion_failures")
local number_calls = real("$dtpq_read_replace_number_calls")
local string_calls = real("$dtpq_read_replace_string_calls")

assert !missing(`fallback')
assert !missing(`conversion_failures')
assert !missing(`number_calls')
assert !missing(`string_calls')
assert `number_calls' > 0
assert `string_calls' > 0
assert `fallback' == 0
assert `conversion_failures' == 0

display as result "T05 typed transfer writer check passed"
display as result "fallback_calls=`fallback' conversion_failures=`conversion_failures'"

log close
cd "`initial_pwd'"
