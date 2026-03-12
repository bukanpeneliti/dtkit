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
postfile `posth' str10 engine int run byte warmup double seconds long nobs int nvars ///
    double scanplan_ms open_ms collect_ms cast_ms sink_ms execute_ms ///
    double stata_plugin_ms stata_strl_fix_ms stata_meta_ms stata_foreign_cat_ms ///
    double stata_describe_ms stata_loadmeta_ms stata_varprep_ms stata_mapping_ms ///
    double stata_filevars_ms stata_matchwin_ms stata_genrecast_ms stata_readfields_ms stata_castjson_ms ///
    using "`results'", replace

forvalues r = 1/`total' {
    local is_warmup = (`r' <= `warmup')

    clear
    timer clear 1
    timer on 1
    quietly dtparquet use `parquet_file', clear notimer
    timer off 1
    quietly timer list 1
    local t1 = r(t1)
    local m_scanplan = real("$read_scan_plan_elapsed_ms")
    local m_open = real("$read_open_scan_elapsed_ms")
    local m_collect = real("$read_collect_elapsed_ms")
    local m_cast = real("$read_apply_cast_elapsed_ms")
    local m_sink = real("$read_sink_to_stata_elapsed_ms")
    local m_execute = real("$read_execute_elapsed_ms")
    local s_plugin = real("$dtpq_use_plugin_ms")
    local s_strl_fix = real("$dtpq_use_strl_ms")
    local s_meta = real("$dtpq_use_meta_ms")
    local s_foreign_cat = real("$dtpq_use_cat_ms")
    local s_describe = real("$dtpq_use_describe_ms")
    local s_loadmeta = real("$dtpq_use_loadmeta_ms")
    local s_varprep = real("$dtpq_use_varprep_ms")
    local s_mapping = real("$dtpq_use_mapping_ms")
    local s_filevars = real("$dtpq_use_filevars_ms")
    local s_matchwin = real("$dtpq_use_matchwin_ms")
    local s_genrecast = real("$dtpq_use_genrecast_ms")
    local s_readfields = real("$dtpq_use_readfields_ms")
    local s_castjson = real("$dtpq_use_castjson_ms")
    post `posth' ("dtparquet") (`r') (`is_warmup') (`t1') (_N) (c(k)) ///
        (`m_scanplan') (`m_open') (`m_collect') (`m_cast') (`m_sink') (`m_execute') ///
        (`s_plugin') (`s_strl_fix') (`s_meta') (`s_foreign_cat') ///
        (`s_describe') (`s_loadmeta') (`s_varprep') (`s_mapping') ///
        (`s_filevars') (`s_matchwin') (`s_genrecast') (`s_readfields') (`s_castjson')

    if `has_pq' {
        clear
        timer clear 2
        timer on 2
        quietly pq use `parquet_file', clear
        timer off 2
        quietly timer list 2
        local t2 = r(t2)
        post `posth' ("pq") (`r') (`is_warmup') (`t2') (_N) (c(k)) ///
            (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.)
    }

    clear
    timer clear 3
    timer on 3
    quietly use `dta_file', clear
    timer off 3
    quietly timer list 3
    local t3 = r(t3)
    post `posth' ("stata") (`r') (`is_warmup') (`t3') (_N) (c(k)) ///
        (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.) (.)
}

postclose `posth'
use "`results'", clear

display _newline "Raw timing runs"
list engine run warmup seconds nobs nvars, sepby(engine) noobs

keep if warmup == 0

preserve
keep if engine == "dtparquet"
collapse (mean) mean_scanplan_ms=scanplan_ms mean_open_ms=open_ms mean_collect_ms=collect_ms ///
    mean_cast_ms=cast_ms mean_sink_ms=sink_ms mean_execute_ms=execute_ms ///
    mean_stata_plugin_ms=stata_plugin_ms mean_stata_strl_fix_ms=stata_strl_fix_ms ///
    mean_stata_meta_ms=stata_meta_ms mean_stata_foreign_cat_ms=stata_foreign_cat_ms ///
    mean_stata_describe_ms=stata_describe_ms mean_stata_loadmeta_ms=stata_loadmeta_ms ///
    mean_stata_varprep_ms=stata_varprep_ms mean_stata_mapping_ms=stata_mapping_ms ///
    mean_stata_filevars_ms=stata_filevars_ms mean_stata_matchwin_ms=stata_matchwin_ms ///
    mean_stata_genrecast_ms=stata_genrecast_ms mean_stata_readfields_ms=stata_readfields_ms ///
    mean_stata_castjson_ms=stata_castjson_ms
display _newline "dtparquet phase means over measured runs (ms)"
list mean_scanplan_ms mean_open_ms mean_collect_ms mean_cast_ms mean_sink_ms mean_execute_ms ///
    mean_stata_plugin_ms mean_stata_strl_fix_ms mean_stata_meta_ms mean_stata_foreign_cat_ms ///
    mean_stata_describe_ms mean_stata_loadmeta_ms mean_stata_varprep_ms mean_stata_mapping_ms ///
    mean_stata_filevars_ms mean_stata_matchwin_ms mean_stata_genrecast_ms mean_stata_readfields_ms ///
    mean_stata_castjson_ms, noobs
restore

collapse (mean) mean_seconds=seconds (p50) p50_seconds=seconds (min) min_seconds=seconds (max) max_seconds=seconds, by(engine)

display _newline "Summary over measured runs"
list engine mean_seconds p50_seconds min_seconds max_seconds, noobs

display _newline "Done. Log written to: ado/ancillary_files/test/log/dtparquet_repro_pihps.log"

log close
cd "`initial_pwd'"
