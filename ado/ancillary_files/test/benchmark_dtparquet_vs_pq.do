*! benchmark_dtparquet_vs_pq.do
*! Baseline benchmark harness for dtparquet

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

if !fileexists("ado/dtparquet.ado") {
    display as error "Missing ado/dtparquet.ado. Run from repository root."
    exit 601
}

if !fileexists("ado/ancillary_files/dtparquet.dll") {
    display as error "Missing ado/ancillary_files/dtparquet.dll"
    exit 601
}

adopath ++ "ado"

local log_root "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/log"

log using "`log_root'/benchmark_dtparquet_vs_pq.log", text replace

local warmup_runs 2
local measured_runs 8
local total_runs = `warmup_runs' + `measured_runs'
local chunk_size 50000

tempfile run_results
tempname posth

postfile `posth' ///
    str20 scenario ///
    str8 operation ///
    int run ///
    byte warmup ///
    double elapsed ///
    long src_nobs ///
    int src_nvars ///
    long out_nobs ///
    int out_nvars ///
    double collect_calls ///
    double planned_batches ///
    double processed_batches ///
    double replace_number_calls ///
    double replace_string_calls ///
    double pull_numeric_calls ///
    double pull_string_calls ///
    double pull_strl_calls ///
    double queue_capacity ///
    double queue_peak ///
    double queue_bp_events ///
    double queue_wait_ms ///
    double queue_prod_batches ///
    double queue_cons_batches ///
    double strl_trunc_events ///
    double strl_binary_events ///
    using "`run_results'", replace

foreach scenario in narrow_numeric wide_mixed string_heavy {
    display as text "Running scenario: `scenario'"

    clear
    set seed 12345

    if "`scenario'" == "narrow_numeric" {
        set obs 500000
        gen long id = _n
        forvalues j = 1/6 {
            gen long int`j' = mod(_n + `j', 1000000)
        }
        forvalues j = 1/6 {
            gen double dbl`j' = (runiform() * 10000) + `j'
        }
    }
    else if "`scenario'" == "wide_mixed" {
        set obs 100000
        gen long id = _n
        forvalues j = 1/80 {
            gen long i`j' = mod((_n * `j') + 17, 1000000)
        }
        forvalues j = 1/60 {
            gen double d`j' = (runiform() * 1000) + `j'
        }
        forvalues j = 1/40 {
            gen str24 s`j' = "g" + string(mod(_n + `j', 10000), "%04.0f")
        }
    }
    else if "`scenario'" == "string_heavy" {
        set obs 40000
        gen long id = _n
        gen str32 code = "id_" + string(_n, "%08.0f")
        gen str120 text_short = "entry_" + string(mod(_n, 97)) + "_" + string(mod(_n, 991))
        gen strL text_long = ""
        forvalues j = 1/70 {
            replace text_long = text_long + "abcdefghijklmnopqrstuvwxzy0123456789"
        }
        replace text_long = text_long + "_" + string(_n, "%08.0f")
        forvalues j = 1/10 {
            gen str80 note`j' = "note_" + string(mod(_n * `j', 100000), "%06.0f")
        }
        forvalues j = 1/6 {
            gen double v`j' = runiform() * (`j' + 1)
        }
    }
    else {
        display as error "Unknown scenario: `scenario'"
        exit 198
    }

    local src_nobs = _N
    local src_nvars = c(k)

    tempfile source_data scenario_path
    quietly save "`source_data'", replace
    local parquet_file "`scenario_path'.parquet"

    forvalues run = 1/`total_runs' {
        local is_warmup = (`run' <= `warmup_runs')

        quietly use "`source_data'", clear
        timer clear 1
        timer on 1
        quietly dtparquet save "`scenario_path'", replace chunksize(`chunk_size')
        timer off 1
        quietly timer list 1
        local elapsed = r(t1)

        local out_nobs = _N
        local out_nvars = c(k)

        local collect_calls = real("$write_collect_calls")
        if missing(`collect_calls') local collect_calls = 0
        local planned_batches = real("$write_planned_batches")
        if missing(`planned_batches') local planned_batches = 0
        local processed_batches = real("$write_processed_batches")
        if missing(`processed_batches') local processed_batches = 0
        local replace_number_calls = real("$write_replace_number_calls")
        if missing(`replace_number_calls') local replace_number_calls = 0
        local replace_string_calls = real("$write_replace_string_calls")
        if missing(`replace_string_calls') local replace_string_calls = 0
        local pull_numeric_calls = real("$write_pull_numeric_calls")
        if missing(`pull_numeric_calls') local pull_numeric_calls = 0
        local pull_string_calls = real("$write_pull_string_calls")
        if missing(`pull_string_calls') local pull_string_calls = 0
        local pull_strl_calls = real("$write_pull_strl_calls")
        if missing(`pull_strl_calls') local pull_strl_calls = 0
        local queue_capacity = real("$write_queue_capacity")
        if missing(`queue_capacity') local queue_capacity = 0
        local queue_peak = real("$write_queue_peak")
        if missing(`queue_peak') local queue_peak = 0
        local queue_bp_events = real("$write_queue_bp_events")
        if missing(`queue_bp_events') local queue_bp_events = 0
        local queue_wait_ms = real("$write_queue_wait_ms")
        if missing(`queue_wait_ms') local queue_wait_ms = 0
        local queue_prod_batches = real("$write_queue_prod_batches")
        if missing(`queue_prod_batches') local queue_prod_batches = 0
        local queue_cons_batches = real("$write_queue_cons_batches")
        if missing(`queue_cons_batches') local queue_cons_batches = 0
        local strl_trunc_events = real("$write_strl_trunc_events")
        if missing(`strl_trunc_events') local strl_trunc_events = 0
        local strl_binary_events = real("$write_strl_binary_events")
        if missing(`strl_binary_events') local strl_binary_events = 0

        post `posth' ///
            ("`scenario'") ///
            ("save") ///
            (`run') ///
            (`is_warmup') ///
            (`elapsed') ///
            (`src_nobs') ///
            (`src_nvars') ///
            (`out_nobs') ///
            (`out_nvars') ///
            (`collect_calls') ///
            (`planned_batches') ///
            (`processed_batches') ///
            (`replace_number_calls') ///
            (`replace_string_calls') ///
            (`pull_numeric_calls') ///
            (`pull_string_calls') ///
            (`pull_strl_calls') ///
            (`queue_capacity') ///
            (`queue_peak') ///
            (`queue_bp_events') ///
            (`queue_wait_ms') ///
            (`queue_prod_batches') ///
            (`queue_cons_batches') ///
            (`strl_trunc_events') ///
            (`strl_binary_events')

        clear
        timer clear 2
        timer on 2
        quietly dtparquet use using "`parquet_file'", clear chunksize(`chunk_size')
        timer off 2
        quietly timer list 2
        local elapsed = r(t2)

        local out_nobs = _N
        local out_nvars = c(k)

        assert _N == `src_nobs'
        assert c(k) == `src_nvars'

        local collect_calls = real("$read_collect_calls")
        if missing(`collect_calls') local collect_calls = 0
        local planned_batches = real("$read_planned_batches")
        if missing(`planned_batches') local planned_batches = 0
        local processed_batches = real("$read_processed_batches")
        if missing(`processed_batches') local processed_batches = 0
        local replace_number_calls = real("$read_replace_number_calls")
        if missing(`replace_number_calls') local replace_number_calls = 0
        local replace_string_calls = real("$read_replace_string_calls")
        if missing(`replace_string_calls') local replace_string_calls = 0
        local pull_numeric_calls = real("$read_pull_numeric_calls")
        if missing(`pull_numeric_calls') local pull_numeric_calls = 0
        local pull_string_calls = real("$read_pull_string_calls")
        if missing(`pull_string_calls') local pull_string_calls = 0
        local pull_strl_calls = real("$read_pull_strl_calls")
        if missing(`pull_strl_calls') local pull_strl_calls = 0
        local queue_capacity = 0
        local queue_peak = 0
        local queue_bp_events = 0
        local queue_wait_ms = 0
        local queue_prod_batches = 0
        local queue_cons_batches = 0
        local strl_trunc_events = 0
        local strl_binary_events = 0

        post `posth' ///
            ("`scenario'") ///
            ("use") ///
            (`run') ///
            (`is_warmup') ///
            (`elapsed') ///
            (`src_nobs') ///
            (`src_nvars') ///
            (`out_nobs') ///
            (`out_nvars') ///
            (`collect_calls') ///
            (`planned_batches') ///
            (`processed_batches') ///
            (`replace_number_calls') ///
            (`replace_string_calls') ///
            (`pull_numeric_calls') ///
            (`pull_string_calls') ///
            (`pull_strl_calls') ///
            (`queue_capacity') ///
            (`queue_peak') ///
            (`queue_bp_events') ///
            (`queue_wait_ms') ///
            (`queue_prod_batches') ///
            (`queue_cons_batches') ///
            (`strl_trunc_events') ///
            (`strl_binary_events')
    }
}

postclose `posth'

use "`run_results'", clear
order scenario operation run warmup elapsed src_nobs src_nvars out_nobs out_nvars
save "`log_root'/benchmark_baseline_runs.dta", replace
export delimited using "`log_root'/benchmark_baseline_runs.csv", replace

preserve
keep if warmup == 0
collapse (count) n_runs=elapsed ///
    (mean) mean_elapsed=elapsed ///
    (sd) sd_elapsed=elapsed ///
    (p50) p50_elapsed=elapsed ///
    (p90) p90_elapsed=elapsed ///
    (mean) mean_collect_calls=collect_calls ///
    (mean) mean_planned_batches=planned_batches ///
    (mean) mean_processed_batches=processed_batches ///
    (mean) mean_replace_number_calls=replace_number_calls ///
    (mean) mean_replace_string_calls=replace_string_calls ///
    (mean) mean_pull_numeric_calls=pull_numeric_calls ///
    (mean) mean_pull_string_calls=pull_string_calls ///
    (mean) mean_pull_strl_calls=pull_strl_calls ///
    (mean) mean_queue_capacity=queue_capacity ///
    (mean) mean_queue_peak=queue_peak ///
    (mean) mean_queue_bp_events=queue_bp_events ///
    (mean) mean_queue_wait_ms=queue_wait_ms ///
    (mean) mean_queue_prod_batches=queue_prod_batches ///
    (mean) mean_queue_cons_batches=queue_cons_batches ///
    (mean) mean_strl_trunc_events=strl_trunc_events ///
    (mean) mean_strl_binary_events=strl_binary_events, by(scenario operation)
gen cv_elapsed = cond(mean_elapsed == 0, ., sd_elapsed / mean_elapsed)
order scenario operation n_runs mean_elapsed p50_elapsed p90_elapsed cv_elapsed
save "`log_root'/benchmark_baseline_summary.dta", replace
export delimited using "`log_root'/benchmark_baseline_summary.csv", replace
list scenario operation n_runs mean_elapsed p50_elapsed p90_elapsed cv_elapsed, noobs
restore

display as result "Run-level output: `log_root'/benchmark_baseline_runs.csv"
display as result "Summary output:   `log_root'/benchmark_baseline_summary.csv"

log close
cd "`initial_pwd'"
