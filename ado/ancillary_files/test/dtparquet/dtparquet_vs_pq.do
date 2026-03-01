*! dtparquet_vs_pq.do
*! Benchmark comparison: dtparquet vs pq (Stata Parquet IO)

version 16
clear all
set more off
discard

local initial_pwd = c(pwd)
cd "D:/OneDrive/MyWork/00personal/stata/dtkit"

* Check dependencies
if !fileexists("ado/dtparquet.ado") {
    display as error "Missing ado/dtparquet.ado"
    exit 601
}

if !fileexists("ado/ancillary_files/dtparquet.dll") {
    display as error "Missing ado/ancillary_files/dtparquet.dll"
    exit 601
}

* Check pq is available
capture which pq
if _rc != 0 {
    display as error "pq command not found. Install with ssc install pq"
    exit 601
}

adopath ++ "ado"

local log_root "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/log"
capture log close

log using "`log_root'/dtparquet_vs_pq.log", text replace

local warmup_runs 2
local measured_runs 8
local total_runs = `warmup_runs' + `measured_runs'
local chunk_size 50000

tempfile run_results
tempname posth

postfile `posth' ///
    str20 scenario ///
    str10 command ///
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
    double write_collect_ms ///
    double write_parquet_ms ///
    using "`run_results'", replace

foreach scenario in narrow_numeric wide_mixed string_heavy {
    display as text "Running scenario: `scenario'"

    clear
    set seed 12345

    if "`scenario'" == "narrow_numeric" {
        quietly set obs 500000
        generate long id = _n
        forvalues j = 1/6 {
            generate long int`j' = mod(_n + `j', 1000000)
        }
        forvalues j = 1/6 {
            generate double dbl`j' = (runiform() * 10000) + `j'
        }
    }
    else if "`scenario'" == "wide_mixed" {
        quietly set obs 100000
        generate long id = _n
        forvalues j = 1/80 {
            generate long i`j' = mod((_n * `j') + 17, 1000000)
        }
        forvalues j = 1/60 {
            generate double d`j' = (runiform() * 1000) + `j'
        }
        forvalues j = 1/40 {
            generate str24 s`j' = "g" + string(mod(_n + `j', 10000), "%04.0f")
        }
    }
    else if "`scenario'" == "string_heavy" {
        quietly set obs 40000
        generate long id = _n
        generate str32 code = "id_" + string(_n, "%08.0f")
        generate str120 text_short = "entry_" + string(mod(_n, 97)) + "_" + string(mod(_n, 991))
        quietly generate strL text_long = ""
        forvalues j = 1/70 {
            quietly replace text_long = text_long + "abcdefghijklmnopqrstuvwxzy0123456789"
        }
        replace text_long = text_long + "_" + string(_n, "%08.0f")
        forvalues j = 1/10 {
            generate str80 note`j' = "note_" + string(mod(_n * `j', 100000), "%06.0f")
        }
        forvalues j = 1/6 {
            generate double v`j' = runiform() * (`j' + 1)
        }
    }
    else {
        display as error "Unknown scenario: `scenario'"
        exit 198
    }

    local src_nobs = _N
    local src_nvars = c(k)

    tempfile source_data dtparquet_path pq_path
    quietly save "`source_data'", replace
    
    local dtparquet_file "`dtparquet_path'.parquet"
    local pq_file "`pq_path'_pq.parquet"

    * ============================================================
    * DTPARQUET SAVE
    * ============================================================
    display as text "  dtparquet save..."
    
    forvalues run = 1/`total_runs' {
        local is_warmup = (`run' <= `warmup_runs')

        quietly use "`source_data'", clear
        timer clear 1
        timer on 1
        quietly dtparquet save "`dtparquet_file'", replace chunksize(`chunk_size')
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
        local write_collect_ms = real("$write_collect_elapsed_ms")
        if missing(`write_collect_ms') local write_collect_ms = 0
        local write_parquet_ms = real("$write_parquet_elapsed_ms")
        if missing(`write_parquet_ms') local write_parquet_ms = 0

        post `posth' ///
            ("`scenario'") ///
            ("dtparquet") ///
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
            (`strl_binary_events') ///
            (`write_collect_ms') ///
            (`write_parquet_ms')
    }

    * ============================================================
    * DTPARQUET USE
    * ============================================================
    display as text "  dtparquet use..."
    
    forvalues run = 1/`total_runs' {
        local is_warmup = (`run' <= `warmup_runs')

        clear
        timer clear 2
        timer on 2
        quietly dtparquet use using "`dtparquet_file'", clear chunksize(`chunk_size')
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
        local write_collect_ms = 0
        local write_parquet_ms = 0

        post `posth' ///
            ("`scenario'") ///
            ("dtparquet") ///
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
            (`strl_binary_events') ///
            (`write_collect_ms') ///
            (`write_parquet_ms')
    }

    * ============================================================
    * PQ SAVE
    * ============================================================
    display as text "  pq save..."
    
    forvalues run = 1/`total_runs' {
        local is_warmup = (`run' <= `warmup_runs')

        quietly use "`source_data'", clear
        timer clear 1
        timer on 1
        quietly pq save using "`pq_file'", replace
        timer off 1
        quietly timer list 1
        local elapsed = r(t1)

        local out_nobs = _N
        local out_nvars = c(k)

        local collect_calls = 0
        local planned_batches = 0
        local processed_batches = 0
        local replace_number_calls = 0
        local replace_string_calls = 0
        local pull_numeric_calls = 0
        local pull_string_calls = 0
        local pull_strl_calls = 0
        local queue_capacity = 0
        local queue_peak = 0
        local queue_bp_events = 0
        local queue_wait_ms = 0
        local queue_prod_batches = 0
        local queue_cons_batches = 0
        local strl_trunc_events = 0
        local strl_binary_events = 0
        local write_collect_ms = 0
        local write_parquet_ms = 0

        post `posth' ///
            ("`scenario'") ///
            ("pq") ///
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
            (`strl_binary_events') ///
            (`write_collect_ms') ///
            (`write_parquet_ms')
    }

    * ============================================================
    * PQ USE
    * ============================================================
    display as text "  pq use..."
    
    forvalues run = 1/`total_runs' {
        local is_warmup = (`run' <= `warmup_runs')

        clear
        timer clear 2
        timer on 2
        quietly pq use using "`pq_file'", clear
        timer off 2
        quietly timer list 2
        local elapsed = r(t2)

        local out_nobs = _N
        local out_nvars = c(k)

        assert _N == `src_nobs'
        assert c(k) == `src_nvars'

        local collect_calls = 0
        local planned_batches = 0
        local processed_batches = 0
        local replace_number_calls = 0
        local replace_string_calls = 0
        local pull_numeric_calls = 0
        local pull_string_calls = 0
        local pull_strl_calls = 0
        local queue_capacity = 0
        local queue_peak = 0
        local queue_bp_events = 0
        local queue_wait_ms = 0
        local queue_prod_batches = 0
        local queue_cons_batches = 0
        local strl_trunc_events = 0
        local strl_binary_events = 0
        local write_collect_ms = 0
        local write_parquet_ms = 0

        post `posth' ///
            ("`scenario'") ///
            ("pq") ///
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
            (`strl_binary_events') ///
            (`write_collect_ms') ///
            (`write_parquet_ms')
    }
    
    * Cleanup parquet files
    capture erase "`dtparquet_file'"
    capture erase "`pq_file'"
}

postclose `posth'

use "`run_results'", clear

* Summary by scenario/command/operation - display only
display _newline(2) "=============================================="
display "BENCHMARK SUMMARY (mean of `measured_runs' runs)"
display "=============================================="

keep if warmup == 0

display _newline(1) "DTPARQUET INTERNAL METRICS (mean over measured runs)"
preserve
keep if command == "dtparquet"
collapse (mean) mean_elapsed=elapsed ///
    mean_collect=collect_calls ///
    mean_planned=planned_batches ///
    mean_processed=processed_batches ///
    mean_rep_num=replace_number_calls ///
    mean_rep_str=replace_string_calls ///
    mean_pull_num=pull_numeric_calls ///
    mean_pull_str=pull_string_calls ///
    mean_pull_strl=pull_strl_calls ///
    mean_write_collect_ms=write_collect_ms ///
    mean_write_parquet_ms=write_parquet_ms, by(scenario operation)
list scenario operation mean_elapsed mean_collect mean_planned mean_processed mean_rep_num mean_rep_str mean_pull_num mean_pull_str mean_pull_strl mean_write_collect_ms mean_write_parquet_ms, noobs
restore

collapse (mean) mean_elapsed=elapsed (p50) p50_elapsed=elapsed (p90) p90_elapsed=elapsed, by(scenario command operation)

* Display summary table
list scenario command operation mean_elapsed p50_elapsed p90_elapsed, noobs

* Speedup comparison
display _newline(2) "=============================================="
display "SPEEDUP RATIO (pq time / dtparquet time)"
display "=============================================="
display "> 1 means dtparquet is faster"
display ""

reshape wide mean_elapsed p50_elapsed p90_elapsed, i(scenario operation) j(command) string

gen ratio = mean_elapsedpq / mean_elapseddtparquet
list scenario operation ratio, noobs

display _newline "Values > 1 indicate dtparquet is faster than pq"

log close
cd "`initial_pwd'"
