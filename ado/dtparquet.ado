*! Version 1.1.0 01Feb2026
program dtparquet
    version 16
    _cleanup_orphaned
    gettoken sub rest : 0
    local len = length("`sub'")
    if `len' == 0 {
        display as error "Subcommand required: save, use, export, or import"
        exit 198
    }

    if "`sub'" == substr("save", 1, max(2, `len')) {
        dtparquet_save `rest'
    }
    else if "`sub'" == substr("use", 1, max(1, `len')) {
        dtparquet_use `rest'
    }
    else if "`sub'" == substr("export", 1, max(3, `len')) {
        dtparquet_export `rest'
    }
    else if "`sub'" == substr("import", 1, max(3, `len')) {
        dtparquet_import `rest'
    }
    else {
        display as error "Unknown subcommand '`sub''"
        
        // Inference logic
        local has_using = strpos(`"`rest'"', " using ") > 0 | substr(trim(`"`rest'"'), 1, 5) == "using"
        
        if `has_using' {
            // If there's a using, it's likely use, export, or import
            // If the unknown sub looks like a variable or part of a varlist, suggest 'use'
            display as error "Did you mean 'use', 'export', or 'import'?"
            display as error "Try, for example: "
            display as smcl `"{stata dtparquet use `0'}"'
        }
        else {
            // No using, likely 'save' or a typo in a subcommand
            display as error "Did you mean 'save'?"
            display as error "Try, for example: "
            display as smcl `"{stata dtparquet save `0'}"'
        }
        exit 198
    }
end

cap program drop dtparquet_plugin
program dtparquet_plugin, plugin using("ado/ancillary_files/dtparquet.dll")

capture program drop dtparquet_save
program dtparquet_save
    syntax anything(name=filename) [, REplace NOLabel CHunksize(integer 50000)]
    local is_nolabel = ("`nolabel'" != "")
    local file = subinstr(`"`filename'"', `"""', "", .)
    local file : subinstr local file "\" "/", all

    if lower(substr("`file'", -8, .)) == ".parquet" {
        local file = substr("`file'", 1, length("`file'") - 8)
    }
    local file "`file'.parquet"

    if "`replace'" == "" confirm new file `"`file'"'
    if `is_nolabel' == 0 {
        capture which dtmeta
        if _rc == 0 {
            quietly dtmeta
        }
        else {
            display as warn "dtmeta not found, saving without extended metadata."
            local is_nolabel 1
        }
    }

    quietly ds
    local varlist `r(varlist)'
    local var_count: word count `varlist'

    local i = 0
    foreach vari of local varlist {
        local i = `i' + 1
        local typei: type `vari'
        local formati: format `vari'
        local str_length 0

        if ((substr("`typei'", 1, 3) == "str") & ("`typei'" != "strl")) {
            local str_length = substr("`typei'", 4, .)
            local typei string
        }

        local name_`i' `vari'
        local dtype_`i' `typei'
        local format_`i' `formati'
        local str_length_`i' `str_length'
    }

    plugin call dtparquet_plugin, "save" "`file'" "from_macro" "0" "0" "" "from_macros" "" "zstd" "-1" "1" "0" "0"

    if `is_nolabel' == 0 {
        foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
            capture frame drop `fr'
        }
        capture error 0
    }
end

capture program drop dtparquet_use
program dtparquet_use
    version 16

    syntax [anything(everything)] [, Clear NOLabel CHunksize(string) ALLstring]
    
    local vlist ""
    local if_exp ""
    local in_exp ""
    local filename ""
    
    local rest `"`anything'"'
    while `"`rest'"' != "" {
        gettoken tok rest : rest
        
        if `"`tok'"' == "if" {
            local if_exp "if "
            gettoken tok rest : rest
            while `"`tok'"' != "" & !inlist(`"`tok'"', "in", "using") {
                local if_exp `"`if_exp'`tok' "'
                gettoken tok rest : rest
            }
            if inlist(`"`tok'"', "in", "using") local rest `"`tok' `rest'"'
        }
        else if `"`tok'"' == "in" {
            local in_exp "in "
            gettoken tok rest : rest
            while `"`tok'"' != "" & !inlist(`"`tok'"', "if", "using") {
                local in_exp `"`in_exp'`tok' "'
                gettoken tok rest : rest
            }
            if inlist(`"`tok'"', "if", "using") local rest `"`tok' `rest'"'
        }
        else if `"`tok'"' == "using" {
            gettoken filename rest : rest
        }
        else {
            if `"`filename'"' == "" {
                local vlist `"`vlist'`tok' "'
            }
        }
    }
    
    local vlist = trim(`"`vlist'"')
    if `"`filename'"' == "" {
        local filename `"`vlist'"'
        local vlist ""
    }

    local is_nolabel = ("`nolabel'" != "")
    local is_clear = ("`clear'" != "")
    local is_int64_as_string = ("`allstring'" != "")

    local file = subinstr(trim(`"`filename'"'), `"""', "", .)
    local file : subinstr local file "\" "/", all

    if lower(substr("`file'", -8, .)) == ".parquet" {
        local file = substr("`file'", 1, length("`file'") - 8)
    }
    local file "`file'.parquet"

    confirm file `"`file'"'
    if `is_clear' == 0 & (c(N) > 0 | c(k) > 0) error 4
    if `is_clear' == 1 quietly clear

    plugin call dtparquet_plugin, "describe" "`file'" "1" "0" "" "" "0" "0"

    local n_rows = `n_rows'
    local n_columns = `n_columns'
    local vars_in_file
    forvalues i = 1/`n_columns' {
        local vars_in_file `vars_in_file' `name_`i''
    }

    if "`vlist'" == "" | "`vlist'" == "*" {
        local matched_vars `vars_in_file'
    }
    else {
        dtparquet_match_variables `vlist', against(`vars_in_file')
        local matched_vars `r(matched_vars)'
    }

    local offset 0
    local last_n 0
    if "`in_exp'" != "" {
        local in_clean = trim(subinstr(`"`in_exp'"', "in", "", 1))
        local slash = strpos("`in_clean'", "/")
        if `slash' > 0 {
            local offset = real(substr("`in_clean'", 1, `slash' - 1))
            local last_n = real(substr("`in_clean'", `slash' + 1, .))
        }
    }

    if (`last_n' == 0) local last_n = `n_rows'
    local row_to_read = max(0, min(`n_rows', `last_n') - `offset' + (`offset' > 0))
    local n_obs_already = _N
    local n_obs_after = `n_obs_already' + `row_to_read'
    quietly set obs `n_obs_after'

    local match_vars_non_binary
    local cast_string_vars
    foreach vari in `matched_vars' {
        local var_number: list posof "`vari'" in vars_in_file
        local type `type_`var_number''
        local p_type `polars_type_`var_number''
        local string_length `string_length_`var_number''

        if (`is_int64_as_string' & inlist("`p_type'", "int64", "uint64")) {
            local type strl
            local cast_string_vars `cast_string_vars' `vari'
        }
        local load_type_`var_number' `type'

        dtparquet_gen_or_recast,  name(`vari')        ///
                                type_new(`type')     ///
                                str_length(`string_length')

        if ("`type'" == "datetime") {
            format `vari' %tc
        }
        else if ("`type'" == "date") {
            format `vari' %td
        }
        else if ("`type'" == "time") {
            format `vari' %tchh:mm:ss
        }
        else if ("`type'" == "binary") {
            continue
        }

        local match_vars_non_binary `match_vars_non_binary' `vari'
    }

    local matched_vars `match_vars_non_binary'
    local n_matched_vars: word count `matched_vars'

    local i = 0
    foreach vari of varlist * {
        local i = `i' + 1
        local i_matched : list posof "`vari'" in matched_vars
        if (`i_matched' > 0) {
            local i_original : list posof "`vari'" in vars_in_file
            local v_to_read_index_`i_matched' `i'
            local v_to_read_name_`i_matched' `vari'
            local v_to_read_type_`i_matched' `load_type_`i_original''
            local v_to_read_p_type_`i_matched' `polars_type_`i_original''
        }
    }

    local cast_json ""

    local mapping from_macros
    local parallelize ""
    local vertical_relaxed 0
    local asterisk_to_variable ""
    local sort ""
    local sql_if ""
    local batch_size = cond("`chunksize'" == "", 50000, real("`chunksize'"))
    local plugin_offset = max(0, `offset' - 1)

    plugin call dtparquet_plugin, "read" "`file'" "from_macro" "`row_to_read'" "`plugin_offset'" "`sql_if'" "`mapping'" "`parallelize'" "`vertical_relaxed'" "`asterisk_to_variable'" "`sort'" "`n_obs_already'" "0" "0" "`batch_size'"
    
    local if_in = trim("`if_exp' `in_exp'")
    if `"`if_in'"' != "" quietly keep `if_in'
    if `is_nolabel' == 0 {
        capture confirm frame _dtvars
        if _rc == 0 {
            _apply_dtmeta
        }
        else {
            capture error 0
        }
    }
end

capture program drop dtparquet_export
program dtparquet_export
    _check_python
    syntax anything(name=pqfile) using/ [, REplace NOLabel CHunksize(integer 50000)]
    local is_nolabel = ("`nolabel'" != "")
    
    local target = subinstr(trim(`"`pqfile'"'), `"""', "", .)
    local target : subinstr local target "\" "/", all
    local source = subinstr(trim(`"`using'"'), `"""', "", .)
    local source : subinstr local source "\" "/", all

    if lower(substr("`target'", -8, .)) == ".parquet" {
        local target = substr("`target'", 1, length("`target'") - 8)
    }
    local target "`target'.parquet"
    
    confirm file `"`source'"'
    if "`replace'" == "" confirm new file `"`target'"'
    
    local orig_frame = c(frame)
    
    // 1. Initialize Stream
    python: import dtparquet
    python: dtparquet.StreamManager.init_export("`target'", bool(`is_nolabel'))
    
    // 2. Metadata Phase (using first observation)
    tempname metadata_frame
    frame create `metadata_frame'
    quietly frame `metadata_frame': use `"`source'"' in 1/1, clear
    
    if `is_nolabel' == 0 {
        frame `metadata_frame': capture which dtmeta
        if _rc == 0 {
            quietly frame `metadata_frame': dtmeta
        }
    }
    
    // 3. Streaming Phase
    quietly describe using `"`source'"'
    local N = r(N)
    
    local start 1
    frame change `metadata_frame'
    while `start' <= `N' {
        local end = min(`start' + `chunksize' - 1, `N')
        quietly use `"`source'"' in `start'/`end', clear
        
        python: dtparquet.StreamManager.write_chunk()
        
        local start = `end' + 1
    }
    
    // 4. Finalize
    python: dtparquet.StreamManager.finalize_export()
    
    // Cleanup
    frame change `orig_frame'
    frame drop `metadata_frame'
    foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
        capture frame drop `fr'
    }
    capture error 0
end

capture program drop dtparquet_import
program dtparquet_import
    _check_python
    syntax anything(name=dtafile) using/ [, REplace NOLabel CHunksize(integer 50000) ALLstring]

    local target = subinstr(trim(`"`dtafile'"'), `"""', "", .)
    local target : subinstr local target "\" "/", all
    local source = subinstr(trim(`"`using'"'), `"""', "", .)
    local source : subinstr local source "\" "/", all

    if lower(substr("`source'", -8, .)) == ".parquet" {
        local source = substr("`source'", 1, length("`source'") - 8)
    }
    local source "`source'.parquet"

    confirm file `"`source'"'
    if "`replace'" == "" confirm new file `"`target'"'

    tempname temp_frame
    local orig_frame = c(frame)
    frame create `temp_frame'
    frame change `temp_frame'

    python: import dtparquet
    python: dtparquet.load_atomic("`source'", bool("`nolabel'" != ""), `chunksize', bool("`allstring'" != ""))

    if "`nolabel'" == "" {
        capture confirm frame _dtvars
        if _rc == 0 {
            _apply_dtmeta
        }
        else {
            capture error 0
        }
    }
    else {
        foreach v of varlist _all {
            label variable `v' ""
            label values `v' .
        }
    }
    quietly save `"`target'"', `replace'

    frame change `orig_frame'
    frame drop `temp_frame'
end

capture program drop _apply_dtmeta
program _apply_dtmeta
    // Restore variable labels and formats from _dtvars
    capture frame _dtvars: count
    if _rc == 0 {
        local nvars = r(N)
        if `nvars' > 0 {
            forvalues i = 1/`nvars' {
                frame _dtvars: local vname = varname[`i']
                capture confirm variable `vname'
                if _rc == 0 {
                    frame _dtvars: local vlab = varlab[`i']
                    frame _dtvars: local vfmt = format[`i']
                    frame _dtvars: local vlbl = vallab[`i']
                    if `"`vlab'"' != "" label variable `vname' `"`vlab'"'
                    if `"`vfmt'"' != "" format `vname' `vfmt'
                    if `"`vlbl'"' != "" capture label values `vname' `vlbl'
                }
            }
        }
    }
    
    // Restore value labels from _dtlabel
    capture frame _dtlabel: count
    if _rc == 0 {
        local nlab_obs = r(N)
        if `nlab_obs' > 0 {
            quietly frame _dtlabel: levelsof vallab, local(lablist)
            foreach lbl of local lablist {
                tempname curlblfr
                frame copy _dtlabel `curlblfr'
                quietly frame `curlblfr': keep if vallab == "`lbl'"
                quietly frame `curlblfr': count
                local nlbls = r(N)
                if `nlbls' > 0 {
                    forvalues j = 1/`nlbls' {
                        frame `curlblfr': local val = value[`j']
                        frame `curlblfr': local txt = label[`j']
                        label define `lbl' `val' `"`txt'"', modify
                    }
                }
                frame drop `curlblfr'
            }
        }
    }
    
    // Restore variable notes from _dtnotes
    capture frame _dtnotes: count
    if _rc == 0 {
        local nnotes = r(N)
        if `nnotes' > 0 {
            forvalues i = 1/`nnotes' {
                frame _dtnotes: local vname = varname[`i']
                capture confirm variable `vname'
                if _rc == 0 {
                    frame _dtnotes: local vnote = _note_text[`i']
                    notes `vname': `"`vnote'"'
                }
            }
        }
    }
    
    // Restore dataset info from _dtinfo
    capture frame _dtinfo: count
    if _rc == 0 {
        local ninfo = r(N)
        if `ninfo' > 0 {
            frame _dtinfo: local dlab = dta_label[1]
            if `"`dlab'"' != "" label data `"`dlab'"'
            
            forvalues i = 1/`ninfo' {
                frame _dtinfo: local dnote = dta_note[`i']
                if `"`dnote'"' != "" notes: `"`dnote'"'
            }
        }
    }
    
    // Cleanup
    foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
        capture frame drop `fr'
    }
end

capture program drop dtparquet_match_variables
program dtparquet_match_variables, rclass
    syntax [anything(name=namelist)], against(string)

    local matched
    local unmatched

    foreach name in `namelist' {
        local found = 0

        if strpos("`name'", "*") | strpos("`name'", "?") {
            foreach v of local against {
                if match("`v'", "`name'") {
                    if strpos(" `matched' ", " `v' ") == 0 {
                        local matched = `" `matched' `v' "'
                    }
                    local found = 1
                }
            }
        }
        else {
            foreach v of local against {
                if "`v'" == "`name'" {
                    if strpos(" `matched' ", " `v' ") == 0 {
                        local matched = `" `matched' `v' "'
                    }
                    local found = 1
                }
            }
        }

        if `found' == 0 {
            local unmatched `unmatched' `name'
        }
    }

    if "`unmatched'" != "" {
        di as error "The following variable(s) were not found: `unmatched'"
        error 111
    }

    return local matched_vars = `"`matched'"'
end

capture program drop dtparquet_gen_or_recast
program dtparquet_gen_or_recast
    version 16
    syntax  ,   name(string)             ///
                type_new(string)         ///
                str_length(integer)

    local string_length = max(1,`str_length')
    if ("`type_new'" == "datetime")      local type_new double
    else if ("`type_new'" == "time")     local type_new double
    else if ("`type_new'" == "date")     local type_new long
    else if ("`type_new'" == "string")   local type_str str`string_length'

    capture confirm variable `name', exact
    local b_gen = _rc > 0

    local vartype
    if (!`b_gen') local vartype: type `name'

    if ("`type_new'" == "string") {
        if `b_gen' {
            quietly gen `type_str' `name' = ""
        }
        else {
            if regexm("`vartype'", "^str([0-9]+)$") {
                local current_length = regexs(1)
                if `string_length' > `current_length' {
                    recast str`string_length' `name'
                }
            }
            else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
                tostring `name', replace force
            }
        }
    }
    else if ("`type_new'" == "strl") {
        if `b_gen' {
            quietly gen strL `name' = ""
        }
        else {
            if regexm("`vartype'", "^str([0-9]+)$") {
                recast strL `name'
            }
            else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
                tostring `name', replace force
                recast strL `name'
            }
        }
    }
    else if ("`type_new'" == "float") {
        if `b_gen' {
            quietly gen float `name' = .
        }
        else {
            if inlist("`vartype'", "long", "double") {
                recast double `name'
            }
            else if inlist("`vartype'", "byte", "int") {
                recast float `name'
            }
        }
    }
    else if ("`type_new'" == "long") {
        if `b_gen' {
            quietly gen long `name' = .
        }
        else {
            if inlist("`vartype'", "byte", "int") {
                recast long `name'
            }
            else if inlist("`vartype'", "float") {
                recast double `name'
            }
        }
    }
    else if ("`type_new'" == "int") {
        if `b_gen' {
            quietly gen int `name' = .
        }
        else {
            if inlist("`vartype'", "byte") {
                recast int `name'
            }
        }
    }
    else if ("`type_new'" == "byte") {
        if `b_gen' {
            quietly gen byte `name' = .
        }
    }
    else if ("`type_new'" == "binary") {
        di "Dropping `name' as cannot process binary columns"
    }
    else {
        if `b_gen' {
            quietly gen double `name' = .
        }
        else {
            if inlist("`vartype'", "byte", "int", "long", "float") {
                recast double `name'
            }
        }
    }
end

capture program drop _check_python
program _check_python
    capture python query
    if _rc != 0 {
        display as error "Python not found."
        exit 198
    }
    capture python which pyarrow
    if _rc != 0 {
        display as error "pyarrow not found."
        exit 198
    }
    local ado_dir = c(sysdir_plus)
    python: import sys, os; sys.path.insert(0, r"`ado_dir'd"); import dtparquet
end

capture program drop _cleanup_orphaned
program _cleanup_orphaned
    version 16
    quietly frame dir
    foreach frame in `r(frames)' {
        if strpos("`frame'", "_dtparquet_") == 1 capture frame drop `frame'
        if inlist("`frame'", "_dtvars", "_dtlabel", "_dtnotes", "_dtinfo") capture frame drop `frame'
    }
    capture python: import dtparquet; dtparquet.cleanup_orphaned_tmp_files()
end
