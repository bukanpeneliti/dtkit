*! benchmark_dtparquet_vs_pq.do
*! Compare performance of dtparquet vs pq

version 16
clear frames
capture log close
cd d:/OneDrive/MyWork/00personal/stata/dtkit

log using ado/ancillary_files/test/log/benchmark_dtparquet_vs_pq.log, replace

// Load programs from ado directory
discard
local ado_plus = c(sysdir_plus)
capture mkdir "`ado_plus'd"
copy ado/dtparquet.ado "`ado_plus'd/dtparquet.ado", replace
copy ado/dtparquet.py "`ado_plus'd/dtparquet.py", replace

// Set up paths
local parquet_file "d:\OneDrive\MyData\bpom\raw\produk\kosmetika\produk_kosmetika_202601.parquet"
local dta_file "d:\OneDrive\MyData\bpom\output\dta\produk_kosmetika.dta"
local log_dir "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\temp"

// Verify files exist
capture confirm file "`parquet_file'"
if _rc {
    display as error "Parquet file not found: `parquet_file'"
    exit 601
}

capture confirm file "`dta_file'"
if _rc {
    display as error "DTA file not found: `dta_file'"
    exit 601
}

// Create temp directory if needed
capture mkdir "`log_dir'"

display _newline(2)
display as text "{hline 80}"
display as result "BENCHMARK: dtparquet vs pq"
display as text "{hline 80}"
display as text "Parquet file: `parquet_file'"
display as text "DTA file:     `dta_file'"
display _newline(1)

// Skip file size display (not critical for benchmark)

// Create temp output files
tempfile temp_out_dtparquet temp_out_pq
local temp_parquet_dtparquet "`temp_out_dtparquet'.parquet"
local temp_parquet_pq "`temp_out_pq'.parquet"

display as text "{hline 80}"
display as result "TEST 1: LOADING PARQUET FILE"
display as text "{hline 80}"
display _newline(1)

// Test 1a: dtparquet use
display as text "Testing dtparquet use..."
timer clear 1
timer on 1
quietly dtparquet use using "`parquet_file'", clear
timer off 1
quietly timer list 1
local time_dtparquet_use = r(t1)
local nobs_dtparquet = _N
local nvars_dtparquet = c(k)
display as result "  Time: " as text %9.3f `time_dtparquet_use' as result " seconds"
display as text "  Observations: " as result %12.0fc `nobs_dtparquet'
display as text "  Variables:    " as result %12.0fc `nvars_dtparquet'
display _newline(1)

// Test 1b: pq use
display as text "Testing pq use..."
timer clear 2
timer on 2
quietly pq use using "`parquet_file'", clear
timer off 2
quietly timer list 2
local time_pq_use = r(t2)
local nobs_pq = _N
local nvars_pq = c(k)
display as result "  Time: " as text %9.3f `time_pq_use' as result " seconds"
display as text "  Observations: " as result %12.0fc `nobs_pq'
display as text "  Variables:    " as result %12.0fc `nvars_pq'
display _newline(1)

// Calculate speedup
local speedup_use = `time_dtparquet_use' / `time_pq_use'
display as text "Results:"
display as result "  pq is " as text %5.2f `speedup_use' as result "x faster" as text " for loading"
display _newline(2)

display as text "{hline 80}"
display as result "TEST 2: SAVING TO PARQUET FILE"
display as text "{hline 80}"
display _newline(1)

// Load the DTA file first
display as text "Loading DTA file for save tests..."
quietly use "`dta_file'", clear
local nobs_save = _N
local nvars_save = c(k)
display as text "  Observations: " as result %12.0fc `nobs_save'
display as text "  Variables:    " as result %12.0fc `nvars_save'
display _newline(1)

// Test 2a: dtparquet save (default chunksize)
display as text "Testing dtparquet save (default chunksize=50000)..."
timer clear 3
timer on 3
quietly dtparquet save "`temp_parquet_dtparquet'", replace
timer off 3
quietly timer list 3
local time_dtparquet_save = r(t3)
display as result "  Time: " as text %9.3f `time_dtparquet_save' as result " seconds"
display _newline(1)

// Test 2b: pq save
display as text "Testing pq save..."
timer clear 4
timer on 4
quietly pq save using "`temp_parquet_pq'", replace
timer off 4
quietly timer list 4
local time_pq_save = r(t4)
display as result "  Time: " as text %9.3f `time_pq_save' as result " seconds"
display _newline(1)

// Calculate speedup
local speedup_save = `time_dtparquet_save' / `time_pq_save'
display as text "Results:"
display as result "  pq is " as text %5.2f `speedup_save' as result "x faster" as text " for saving"
display _newline(2)

// Optional: Test different chunk sizes for dtparquet
display as text "{hline 80}"
display as result "TEST 3: DTPARQUET CHUNK SIZE OPTIMIZATION"
display as text "{hline 80}"
display _newline(1)

foreach chunk in 10000 50000 100000 500000 {
    display as text "Testing dtparquet save with chunksize=`chunk'..."
    timer clear 5
    timer on 5
    quietly dtparquet save "`temp_parquet_dtparquet'", replace chunksize(`chunk')
    timer off 5
    quietly timer list 5
    local time_chunk = r(t5)
    display as result "  Chunksize `chunk': " as text %9.3f `time_chunk' as result " seconds"
}
display _newline(2)

// Summary
display as text "{hline 80}"
display as result "SUMMARY"
display as text "{hline 80}"
display _newline(1)
display as text "Loading parquet file:"
display as text "  dtparquet: " as result %9.3f `time_dtparquet_use' as text " seconds"
display as text "  pq:        " as result %9.3f `time_pq_use' as text " seconds"
display as text "  Speedup:   " as result %5.2f `speedup_use' as text "x (pq faster)"
display _newline(1)
display as text "Saving to parquet file:"
display as text "  dtparquet: " as result %9.3f `time_dtparquet_save' as text " seconds"
display as text "  pq:        " as result %9.3f `time_pq_save' as text " seconds"
display as text "  Speedup:   " as result %5.2f `speedup_use' as text "x (pq faster)"
display _newline(1)
display as text "{hline 80}"

// Cleanup temp files
capture erase "`temp_parquet_dtparquet'"
capture erase "`temp_parquet_pq'"

log close
