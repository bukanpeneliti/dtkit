*! dtparquet_repro_pihps.do
*! Reproduce filename display gap and load-speed comparison on pihps.parquet

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"
adopath ++ "ado"

local data_dir "D:/OneDrive/MyData/pihps/output"
local parquet_file "`data_dir'/pihps.parquet"
local dta_file "`data_dir'/pihps.dta"

capture log close
log using "ado/ancillary_files/test/log/dtparquet_repro_pihps.log", text replace

display "============================================================"
display "dtparquet pihps reproduction"
display "date: `c(current_date)' time: `c(current_time)'"
display "============================================================"
display "parquet: `parquet_file'"
display "dta:     `dta_file'"

if !fileexists("`parquet_file'") {
    display as error "Parquet file not found: `parquet_file'"
    log close
    cd "`initial_pwd'"
    exit 601
}

if !fileexists("`dta_file'") {
    display as error "DTA file not found: `dta_file'"
    log close
    cd "`initial_pwd'"
    exit 601
}

capture which pq
local has_pq = (_rc == 0)

display _newline "[1] Reproduce display message"
display "Running: dtparquet use `parquet_file', clear"
dtparquet use `parquet_file', clear

display _newline "Running: dtparquet use using `parquet_file', clear"
dtparquet use using `parquet_file', clear

display _newline "[2] Speed comparison (1 warmup + 3 measured runs)"
local warmup 1
local measured 3
local total = `warmup' + `measured'

tempname posth
tempfile results
postfile `posth' str10 engine int run byte warmup double seconds long nobs int nvars using "`results'", replace

forvalues r = 1/`total' {
    local is_warmup = (`r' <= `warmup')

    clear
    timer clear 1
    timer on 1
    quietly dtparquet use `parquet_file', clear notimer
    timer off 1
    quietly timer list 1
    local t1 = r(t1)
    post `posth' ("dtparquet") (`r') (`is_warmup') (`t1') (_N) (c(k))

    if `has_pq' {
        clear
        timer clear 2
        timer on 2
        quietly pq use `parquet_file', clear
        timer off 2
        quietly timer list 2
        local t2 = r(t2)
        post `posth' ("pq") (`r') (`is_warmup') (`t2') (_N) (c(k))
    }

    clear
    timer clear 3
    timer on 3
    quietly use `dta_file', clear
    timer off 3
    quietly timer list 3
    local t3 = r(t3)
    post `posth' ("stata") (`r') (`is_warmup') (`t3') (_N) (c(k))
}

postclose `posth'
use "`results'", clear

display _newline "Raw timing runs"
list engine run warmup seconds nobs nvars, sepby(engine) noobs

keep if warmup == 0
collapse (mean) mean_seconds=seconds (p50) p50_seconds=seconds (min) min_seconds=seconds (max) max_seconds=seconds, by(engine)

display _newline "Summary over measured runs"
list engine mean_seconds p50_seconds min_seconds max_seconds, noobs

display _newline "Done. Log written to: ado/ancillary_files/test/log/dtparquet_repro_pihps.log"

log close
cd "`initial_pwd'"
