*! dtparquet_installed_benchmark.do
*! Benchmark comparison: dtparquet (INSTALLED) vs pq
*! Generated on: 10mar2026

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

* Verify installation in PLUS directory
capture which dtparquet
if _rc != 0 {
    display as error "dtparquet is not installed. Run 'net install' first."
    exit 601
}

* Check pq is available
capture which pq
if _rc != 0 {
    display as error "pq command not found. Install with: ssc install pq"
    exit 601
}

* Setup logging in the current working directory
local log_file "dtparquet_benchmark_`c(current_date)'.log"
capture log close
log using "`log_file'", text replace

display "--------------------------------------------------------"
display "Benchmarking INSTALLED dtparquet (v2.0.6 Rust)"
display "Timestamp: `c(current_date)` `c(current_time)`"
display "--------------------------------------------------------"

* --- Parameters ---
local warmup_runs 1
local measured_runs 5
local total_runs = `warmup_runs' + `measured_runs'

* --- Data Generation (1M rows, mixed types) ---
display "Generating 1M rows for benchmark..."
set obs 1000000
gen long id = _n
gen double val_double = runiform()
gen float val_float = runiform()
gen long val_long = runiformint(1, 100000)
gen int val_int = runiformint(1, 1000)
gen str30 val_str = "benchmark_string_" + string(runiformint(1, 1000))

tempfile benchmark_data
save "`benchmark_data'"

tempfile pq_file dtparquet_file

* --- Benchmark Loop ---
tempfile run_results
tempname posth
postfile `posth' str10 command str8 operation int run double time using "`run_results'", replace

foreach cmd in "pq" "dtparquet" {
    display _n "Testing `cmd'..."
    forvalues r = 1/`total_runs' {
        local is_warmup = (`r' <= `warmup_runs')
        local label = cond(`is_warmup', "(warmup)", "(measured)")
        
        * 1. Save benchmark
        preserve
        timer clear 1
        timer on 1
        if "`cmd'" == "pq" {
            pq save "`pq_file'", replace
        }
        else {
            dtparquet save "`dtparquet_file'", replace
        }
        timer off 1
        quietly timer list 1
        local t_save = r(t1)
        restore
        
        if !`is_warmup' post `posth' ("`cmd'") ("save") (`r') (`t_save')
        
        * 2. Use benchmark
        timer clear 2
        timer on 2
        if "`cmd'" == "pq" {
            pq use "`pq_file'", clear
        }
        else {
            dtparquet use "`dtparquet_file'", clear
        }
        timer off 2
        quietly timer list 2
        local t_use = r(t2)
        
        if !`is_warmup' post `posth' ("`cmd'") ("use") (`r') (`t_use')
        
        display "  Run `r' `label': Save=`t_save's, Use=`t_use's"
    }
}

postclose `posth'

* --- Summary Statistics ---
use "`run_results'", clear
display _n "Benchmark Results Summary (Means of `measured_runs' runs)"
display "--------------------------------------------------------"
tabstat time, by(command) statistics(mean sd min max) columns(statistics) format(%9.3f)

display _n "Detailed Comparison:"
list command operation time if operation == "save", sepby(command)
list command operation time if operation == "use", sepby(command)

log close
display _n "Benchmark complete. Results saved to: `log_file'"
