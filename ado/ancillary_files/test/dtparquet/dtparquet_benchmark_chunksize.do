* dtparquet_benchmark_chunksize.do
* Benchmark dtparquet save chunksize choices (display-only output)
* Date: Feb 11, 2026

version 16
clear all
macro drop _all
set more off

cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

log using ado/ancillary_files/test/log/dtparquet_benchmark_chunksize.log, replace

// Install local versions
discard
run "ado/dtparquet.ado"
local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
capture noisily copy "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll" "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.dll"
if _rc != 0 {
    local plugin_dll "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/dtparquet.new.dll"
}
cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("`plugin_dll'")

// Build a huge synthetic dataset to stress save-path batching behavior.
// Tune N to your machine if needed.
local N 10000000
display _newline(2) "=========================================="
display "Starting dtparquet chunksize benchmark"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

clear
set obs `N'
gen long id = _n
gen byte g = mod(_n, 10)
gen int y = 1990 + mod(_n, 30)
gen double x = _n * 1.23456789
gen double z = sqrt(_n) * 3.14159265
gen float f = _n / 7
gen long k = mod(_n, 100000)
gen str80 s = "row_" + string(_n) + "_" + string(mod(_n, 1000))
gen str200 note = "payload_" + string(_n) + "_" + string(mod(_n, 997))

local sizes "50000 100000 200000 400000 800000 0"
local reps 5
local best_size ""
local best_time .

tempfile bench_results
tempname posth
postfile `posth' long chunksize int rep double seconds byte ok using "`bench_results'", replace

display "Benchmark sizes: `sizes'"
display "Note: chunksize(0) uses auto batch size"
display "Rows (N): `N'"
display "Repetitions per chunksize: `reps'"
display "Warm-up: one unmeasured save per chunksize"

foreach cs of local sizes {
    display _newline "--- chunksize(`cs') ---"
    local warm "benchmark_chunksize_`cs'_warmup.parquet"
    capture erase "`warm'"
    capture noisily dtparquet save "`warm'", replace chunksize(`cs')
    if _rc != 0 {
        display as error "warm-up FAILED for chunksize(`cs') with rc=" _rc
    }
    else {
        display as text "warm-up completed for chunksize(`cs')"
    }

    forvalues r = 1/`reps' {
        local out "benchmark_chunksize_`cs'_rep`r'.parquet"
        capture erase "`out'"

        timer clear 1
        timer on 1
        capture noisily dtparquet save "`out'", replace chunksize(`cs')
        local save_rc = _rc
        timer off 1

        if `save_rc' != 0 {
            display as error "rep `r': chunksize(`cs') FAILED with rc=" `save_rc'
            post `posth' (`cs') (`r') (.) (0)
        }
        else {
            timer list 1
            local t = r(t1)
            display as result "rep `r': chunksize(`cs') completed in " %9.3f `t' " sec"
            post `posth' (`cs') (`r') (`t') (1)
        }
    }
}

postclose `posth'

use "`bench_results'", clear

display _newline "=========================================="
display "Bootstrap-like stability summary (by chunksize)"
display "=========================================="

foreach cs of local sizes {
    quietly count if chunksize == `cs' & ok == 1
    local n_ok = r(N)
    if `n_ok' == 0 {
        display as error "chunksize(`cs'): no successful runs"
    }
    else {
        quietly summarize seconds if chunksize == `cs' & ok == 1, meanonly
        local tmin = r(min)
        local tmax = r(max)
        quietly centile seconds if chunksize == `cs' & ok == 1, centile(50)
        local tmed = r(c_1)
        local spread = (`tmax' - `tmin') / `tmed'

        display as result "chunksize(`cs'): median=" %9.3f `tmed' " sec, min=" %9.3f `tmin' " sec, max=" %9.3f `tmax' " sec, spread=" %6.3f `spread'

        if missing(`best_time') | `tmed' < `best_time' {
            local best_time = `tmed'
            local best_size "`cs'"
        }
    }
}

display _newline "=========================================="
if "`best_size'" != "" {
    display as result "Best chunksize by median time: `best_size' (" %9.3f `best_time' " sec)"
}
else {
    display as error "No successful benchmark run."
}
display "=========================================="

// Deterministic cleanup
foreach cs of local sizes {
    capture erase "benchmark_chunksize_`cs'_warmup.parquet"
    forvalues r = 1/`reps' {
        capture erase "benchmark_chunksize_`cs'_rep`r'.parquet"
    }
}

log close
capture erase "dtparquet_benchmark_chunksize.log"
exit 0
