*! Version 1.1.0 13Jan2026
program dtparquet
    version 16
    _cleanup_orphaned
    gettoken sub rest : 0
    if "`sub'" == "save" {
        dtparquet_save `rest'
    }
    else if "`sub'" == "use" {
        dtparquet_use `rest'
    }
    else if "`sub'" == "export" {
        dtparquet_export `rest'
    }
    else if "`sub'" == "import" {
        dtparquet_import `rest'
    }
    else {
        display as error "Unknown subcommand `sub'"
        exit 198
    }
end

capture program drop dtparquet_save
program dtparquet_save
    syntax anything(name=filename) [, replace nolabel chunksize(integer 50000)]
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
    local cmdline `"`0'"'
    gettoken everything options : cmdline, parse(",")
    if substr(`"`options'"', 1, 1) == "," local options = substr(`"`options'"', 2, .)

    // Parse options manually for chunksize and int64_as_string
    local is_nolabel = strpos(`"`options'"', "nolabel") > 0
    local is_clear = strpos(`"`options'"', "clear") > 0
    local is_int64_as_string = strpos(`"`options'"', "int64_as_string") > 0

    local chunksize "None"
    if regexm(`"`options'"', "chunksize\(([0-9]+)\)") {
        local chunksize = regexs(1)
    }

    local upos = strpos(`"`everything'"', " using ")
    if `upos' == 0 {
        if substr(trim(`"`everything'"'), 1, 5) == "using" {
            local prefix ""
            local remainder `"`everything'"'
        }
        else {
            gettoken filename if_in : everything
            local prefix ""
            local remainder ""
        }
    }
    else {
        local prefix = substr(`"`everything'"', 1, `upos'-1)
        local remainder = substr(`"`everything'"', `upos'+1, .)
    }
    if `"`remainder'"' != "" {
        gettoken using_kw remainder : remainder
        gettoken filename if_in_after : remainder
    }
    if `"`filename'"' == "" {
        display as error "using required"
        exit 100
    }
    local vlist ""
    local if_in_before ""
    gettoken tok prefix : prefix
    while `"`tok'"' != "" {
        if inlist(`"`tok'"', "if", "in") {
            local if_in_before `"`tok' `prefix'"'
            local prefix ""
            local tok ""
        }
        else {
            local vlist `"`vlist' `tok'"'
            gettoken tok prefix : prefix
        }
    }
    local if_in = trim(`"`if_in_before' `if_in_after' `if_in'"')
    local file = subinstr(trim(`"`filename'"'), `"""', "", .)
    local file : subinstr local file "\" "/", all
    if `is_clear' == 0 & (c(N) > 0 | c(k) > 0) error 4
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
    python: dtparquet.load("`file'", `py_varlist', bool(`is_nolabel'), `chunksize', bool(`is_int64_as_string'))
    if `"`if_in'"' != "" quietly keep `if_in'
    if `is_nolabel' == 0 _apply_dtmeta
end

capture program drop dtparquet_export
program dtparquet_export
    _check_python
    syntax anything(name=pqfile) using/ [, replace nolabel chunksize(integer 50000)]
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
    syntax anything(name=dtafile) using/ [, replace nolabel chunksize(integer 50000) int64_as_string]

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
    python: dtparquet.load_atomic("`source'", bool("`nolabel'" != ""), `chunksize', bool("`int64_as_string'" != ""))

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
    frame dir
    foreach frame in `r(frames)' {
        if strpos("`frame'", "_dtparquet_") == 1 capture frame drop `frame'
        if inlist("`frame'", "_dtvars", "_dtlabel", "_dtnotes", "_dtinfo") capture frame drop `frame'
    }
    capture python: import dtparquet; dtparquet.cleanup_orphaned_tmp_files()
end
