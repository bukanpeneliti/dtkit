*! dtparquet_test_t03_pool_lifecycle.do

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
adopath ++ "ado"

local log_file "ado/ancillary_files/test/log/dtparquet_test_t03_pool_lifecycle.log"
log using "`log_file'", text replace

local n_rows 200000
local chunk_size 5000

tempfile source_data parquet_stub

set obs `n_rows'
gen long id = _n
forvalues j = 1/16 {
    gen double x`j' = runiform() * `j'
}

save "`source_data'", replace

use "`source_data'", clear
dtparquet save "`parquet_stub'", replace chunksize(`chunk_size')

local save_compute_pool_inits = real("$dtpq_compute_pool_inits")
local save_io_pool_inits = real("$dtpq_io_pool_inits")
assert `save_compute_pool_inits' == 1
assert `save_io_pool_inits' == 1

local parquet_file "`parquet_stub'.parquet"

clear
dtparquet use using "`parquet_file'", clear chunksize(`chunk_size')

local use1_compute_pool_inits = real("$dtpq_compute_pool_inits")
local use1_io_pool_inits = real("$dtpq_io_pool_inits")
local use1_planned_batches = real("$dtpq_read_planned_batches")
local use1_processed_batches = real("$dtpq_read_processed_batches")

assert `use1_compute_pool_inits' == 1
assert `use1_io_pool_inits' == 1
assert `use1_planned_batches' > 1
assert `use1_processed_batches' > 1

clear
dtparquet use using "`parquet_file'", clear chunksize(`chunk_size')

local use2_compute_pool_inits = real("$dtpq_compute_pool_inits")
local use2_io_pool_inits = real("$dtpq_io_pool_inits")

assert `use2_compute_pool_inits' == 1
assert `use2_io_pool_inits' == 1

display as result "T03 lifecycle check passed"
display as result "compute_pool_inits=`use2_compute_pool_inits' io_pool_inits=`use2_io_pool_inits'"
display as result "planned_batches=`use1_planned_batches' processed_batches=`use1_processed_batches'"

log close
cd "`initial_pwd'"
