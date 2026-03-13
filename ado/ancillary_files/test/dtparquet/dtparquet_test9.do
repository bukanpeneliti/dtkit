*! dtparquet_test9.do
*! Combined pihps repro + matrix benchmark

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

if !fileexists("`parquet_file'") {
    display as error "Parquet file not found: `parquet_file'"
    cd "`initial_pwd'"
    exit 601
}
if !fileexists("`dta_file'") {
    display as error "DTA file not found: `dta_file'"
    cd "`initial_pwd'"
    exit 601
}

capture which pq
local has_pq = (_rc == 0)

capture log close
log using "ado/ancillary_files/test/log/dtparquet_test9.log", text replace

display "============================================================"
display "dtparquet test9 (repro + matrix)"
display "date: `c(current_date)' time: `c(current_time)'"
display "parquet: `parquet_file'"
display "dta:     `dta_file'"
display "============================================================"

display _newline "[1] Reproduce display message"
display "Running: dtparquet use `parquet_file', clear"
dtparquet use `parquet_file', clear
display _newline "Running: dtparquet use using `parquet_file', clear"
dtparquet use using `parquet_file', clear

display _newline "[2] Matrix benchmark (parallel x chunksize)"
local warmup 1
local measured 3
local total = `warmup' + `measured'
local parallel_modes "auto rows columns"
local chunksizes "0 50000 250000"

tempname post_matrix
tempfile matrix_results
postfile `post_matrix' str8 mode long chunksize int run byte warmup double seconds long nobs int nvars ///
    double collect_ms sink_prepare_ms sink_write_ms sink_ms execute_ms selected_batch batch_adjustments ///
    using "`matrix_results'", replace

foreach mode of local parallel_modes {
    foreach chunksize of local chunksizes {
        forvalues r = 1/`total' {
            local is_warmup = (`r' <= `warmup')
            clear
            timer clear 1
            timer on 1
            quietly dtparquet use `parquet_file', clear timer(plugin) parallel(`mode') chunksize(`chunksize')
            timer off 1
            quietly timer list 1
            local t1 = r(t1)
            local m_collect = real("$read_collect_elapsed_ms")
            local m_sink_prepare = real("$read_sink_prepare_elapsed_us") / 1000
            local m_sink_write = real("$read_sink_write_elapsed_us") / 1000
            local m_sink = real("$read_sink_to_stata_elapsed_ms")
            local m_execute = real("$read_execute_elapsed_ms")
            local m_selected_batch = real("$read_selected_batch_size")
            local m_batch_adjustments = real("$read_batch_adjustments")
            post `post_matrix' ("`mode'") (`chunksize') (`r') (`is_warmup') (`t1') (_N) (c(k)) ///
                (`m_collect') (`m_sink_prepare') (`m_sink_write') (`m_sink') (`m_execute') (`m_selected_batch') (`m_batch_adjustments')
        }
    }
}

postclose `post_matrix'
use "`matrix_results'", clear

display _newline "Raw matrix runs"
list mode chunksize run warmup seconds nobs nvars, sepby(mode chunksize) noobs

keep if warmup == 0
collapse (mean) mean_seconds=seconds mean_collect_ms=collect_ms mean_sink_ms=sink_ms ///
    mean_sink_prepare_ms=sink_prepare_ms mean_sink_write_ms=sink_write_ms mean_execute_ms=execute_ms ///
    mean_selected_batch=selected_batch mean_batch_adjustments=batch_adjustments ///
    (min) min_seconds=seconds (max) max_seconds=seconds, by(mode chunksize)
gsort mean_seconds

display _newline "Matrix summary over measured runs"
list mode chunksize mean_seconds min_seconds max_seconds mean_collect_ms mean_sink_prepare_ms mean_sink_write_ms mean_sink_ms mean_execute_ms ///
    mean_selected_batch mean_batch_adjustments, noobs

display _newline "[3] Focused repro (rows + chunksize(0) vs pq vs dta)"
tempname post_repro
tempfile repro_results
postfile `post_repro' str10 engine int run byte warmup double seconds long nobs int nvars ///
    double scanplan_ms open_ms collect_ms cast_ms sink_prepare_ms sink_write_ms sink_ms execute_ms ///
    double stata_plugin_ms stata_strl_fix_ms stata_meta_ms stata_foreign_cat_ms ///
    double stata_describe_ms stata_loadmeta_ms stata_varprep_ms stata_mapping_ms ///
    double stata_filevars_ms stata_matchwin_ms stata_genrecast_ms stata_readfields_ms stata_castjson_ms ///
    using "`repro_results'", replace

forvalues r = 1/`total' {
    local is_warmup = (`r' <= `warmup')

    clear
    timer clear 2
    timer on 2
    quietly dtparquet use `parquet_file', clear timer(plugin) parallel(rows) chunksize(0)
    local s_plugin = r(plugin_ms)
    local s_strl_fix = r(strl_fix_ms)
    local s_meta = r(meta_ms)
    local s_foreign_cat = r(cat_ms)
    local s_describe = r(describe_ms)
    local s_loadmeta = r(loadmeta_ms)
    local s_varprep = r(varprep_ms)
    local s_mapping = r(mapping_ms)
    local s_filevars = r(filevars_ms)
    local s_matchwin = r(matchwin_ms)
    local s_genrecast = r(genrecast_ms)
    local s_readfields = r(readfields_ms)
    local s_castjson = r(castjson_ms)
    timer off 2
    quietly timer list 2
    local t1 = r(t2)
    local m_scanplan = real("$read_scan_plan_elapsed_ms")
    local m_open = real("$read_open_scan_elapsed_ms")
    local m_collect = real("$read_collect_elapsed_ms")
    local m_cast = real("$read_apply_cast_elapsed_ms")
    local m_sink_prepare = real("$read_sink_prepare_elapsed_us") / 1000
    local m_sink_write = real("$read_sink_write_elapsed_us") / 1000
    local m_sink = real("$read_sink_to_stata_elapsed_ms")
    local m_execute = real("$read_execute_elapsed_ms")
    post `post_repro' ("dtparquet") (`r') (`is_warmup') (`t1') (_N) (c(k)) ///
        (`m_scanplan') (`m_open') (`m_collect') (`m_cast') (`m_sink_prepare') (`m_sink_write') (`m_sink') (`m_execute') ///
        (`s_plugin') (`s_strl_fix') (`s_meta') (`s_foreign_cat') ///
        (`s_describe') (`s_loadmeta') (`s_varprep') (`s_mapping') ///
        (`s_filevars') (`s_matchwin') (`s_genrecast') (`s_readfields') (`s_castjson')

    if `has_pq' {
        clear
        timer clear 3
        timer on 3
        quietly pq use `parquet_file', clear
        timer off 3
        quietly timer list 3
        local t2 = r(t3)
        post `post_repro' ("pq") (`r') (`is_warmup') (`t2') (_N) (c(k)) ///
            (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.)
    }

    clear
    timer clear 4
    timer on 4
    quietly use `dta_file', clear
    timer off 4
    quietly timer list 4
    local t3 = r(t4)
    post `post_repro' ("stata") (`r') (`is_warmup') (`t3') (_N) (c(k)) ///
        (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.)
}

postclose `post_repro'
use "`repro_results'", clear
display _newline "Raw focused repro runs"
list engine run warmup seconds nobs nvars, sepby(engine) noobs

keep if warmup == 0
preserve
keep if engine == "dtparquet"
collapse (mean) mean_scanplan_ms=scanplan_ms mean_open_ms=open_ms mean_collect_ms=collect_ms ///
    mean_cast_ms=cast_ms mean_sink_prepare_ms=sink_prepare_ms mean_sink_write_ms=sink_write_ms mean_sink_ms=sink_ms mean_execute_ms=execute_ms ///
    mean_stata_plugin_ms=stata_plugin_ms mean_stata_strl_fix_ms=stata_strl_fix_ms ///
    mean_stata_meta_ms=stata_meta_ms mean_stata_foreign_cat_ms=stata_foreign_cat_ms ///
    mean_stata_describe_ms=stata_describe_ms mean_stata_loadmeta_ms=stata_loadmeta_ms ///
    mean_stata_varprep_ms=stata_varprep_ms mean_stata_mapping_ms=stata_mapping_ms ///
    mean_stata_filevars_ms=stata_filevars_ms mean_stata_matchwin_ms=stata_matchwin_ms ///
    mean_stata_genrecast_ms=stata_genrecast_ms mean_stata_readfields_ms=stata_readfields_ms ///
    mean_stata_castjson_ms=stata_castjson_ms
display _newline "dtparquet focused phase means over measured runs (ms)"
list mean_scanplan_ms mean_open_ms mean_collect_ms mean_cast_ms mean_sink_prepare_ms mean_sink_write_ms mean_sink_ms mean_execute_ms ///
    mean_stata_plugin_ms mean_stata_strl_fix_ms mean_stata_meta_ms mean_stata_foreign_cat_ms ///
    mean_stata_describe_ms mean_stata_loadmeta_ms mean_stata_varprep_ms mean_stata_mapping_ms ///
    mean_stata_filevars_ms mean_stata_matchwin_ms mean_stata_genrecast_ms mean_stata_readfields_ms ///
    mean_stata_castjson_ms, noobs
restore

collapse (mean) mean_seconds=seconds (p50) p50_seconds=seconds (min) min_seconds=seconds (max) max_seconds=seconds, by(engine)
display _newline "Focused repro summary over measured runs"
list engine mean_seconds p50_seconds min_seconds max_seconds, noobs

display _newline "Done. Log written to: ado/ancillary_files/test/log/dtparquet_test9.log"
log close
cd "`initial_pwd'"
