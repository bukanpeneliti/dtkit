*! Version 1.0.0 14Jan2026
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

capture program drop dtparquet_save
program dtparquet_save
    syntax anything(name=filename) [, REplace NOLabel CHunksize(integer 50000)]
    _check_python
    local is_nolabel = ("`nolabel'" != "")
    local file = subinstr(`"`filename'"', `"""', "", .)
    local file : subinstr local file "\" "/", all
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
    python: import dtparquet
    python: dtparquet.save_atomic("`file'", bool(`is_nolabel'), `chunksize')
    if `is_nolabel' == 0 {
        foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
            capture frame drop `fr'
        }
    }
end

capture program drop dtparquet_use
program dtparquet_use
    _check_python
    
    local has_using = strpos(`"`0'"', " using ") > 0 | substr(trim(`"`0'"'), 1, 6) == "using " | trim(`"`0'"') == "using"
    
    if `has_using' {
        syntax [anything(name=vlist)] [if] [in] using/ [, Clear NOLabel CHunksize(string) ALLstring]
        local filename `"`using'"'
    }
    else {
        syntax anything(name=filename) [if] [in] [, Clear NOLabel CHunksize(string) ALLstring]
        local vlist ""
    }

    local chunksize_val = cond("`chunksize'" == "", "None", "`chunksize'")
    local is_nolabel = ("`nolabel'" != "")
    local is_clear = ("`clear'" != "")
    local is_int64_as_string = ("`allstring'" != "")

    local file = subinstr(trim(`"`filename'"'), `"""', "", .)
    local file : subinstr local file "\" "/", all
    if `is_clear' == 0 & (c(N) > 0 | c(k) > 0) error 4
    if `is_clear' == 1 quietly clear
    python: import dtparquet
    if "`vlist'" != "" {
        local py_varlist "["
        local comma ""
        foreach v of local vlist {
            local py_varlist `"`py_varlist'`comma'"`v'""'
            local comma ","
        }
        local py_varlist `"`py_varlist']"'
    }
    else local py_varlist "None"
    python: dtparquet.load("`file'", `py_varlist', bool(`is_nolabel'), `chunksize_val', bool(`is_int64_as_string'))
    
    local if_in = trim("`if' `in'")
    if `"`if_in'"' != "" quietly keep `if_in'
    if `is_nolabel' == 0 _apply_dtmeta
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
end

capture program drop dtparquet_import
program dtparquet_import
    _check_python
    syntax anything(name=dtafile) using/ [, REplace NOLabel CHunksize(integer 50000) ALLstring]

    local target = subinstr(trim(`"`dtafile'"'), `"""', "", .)
    local target : subinstr local target "\" "/", all
    local source = subinstr(trim(`"`using'"'), `"""', "", .)
    local source : subinstr local source "\" "/", all

    confirm file `"`source'"'
    if "`replace'" == "" confirm new file `"`target'"'

    tempname temp_frame
    local orig_frame = c(frame)
    frame create `temp_frame'
    frame change `temp_frame'

    python: import dtparquet
    python: dtparquet.load_atomic("`source'", bool("`nolabel'" != ""), `chunksize', bool("`allstring'" != ""))

    if "`nolabel'" == "" {
        _apply_dtmeta
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
