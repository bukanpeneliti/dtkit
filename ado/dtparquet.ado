*! Version 1.0.0 12Jan2026
program dtparquet
    version 16
    gettoken sub rest : 0, parse(" ,")
    if "`sub'" == "save" {
        dtparquet_save `rest'
    }
    else if "`sub'" == "use" {
        dtparquet_use `rest'
    }
    else if "`sub'" == "export" {
        // Phase 2
        di as error "export not implemented in Phase 1"
        exit 198
    }
    else if "`sub'" == "import" {
        // Phase 2
        di as error "import not implemented in Phase 1"
        exit 198
    }
    else {
        di as error "Unknown subcommand `sub'"
        exit 198
    }
end

program dtparquet_save
    syntax anything(name=filename) [, replace nolabel]
    
    local is_nolabel = ("`nolabel'" != "")
    
    // Handle replace
    local filename = subinstr(`"`filename'"', `"""', "", .)
    local filename : subinstr local filename "\" "/", all
    if "`replace'" == "" {
        confirm new file `"`filename'"'
    }
    
    // Generate metadata if requested
    if `is_nolabel' == 0 {
        capture which dtmeta
        if _rc == 0 {
            dtmeta
        }
        else {
            di as warn "dtmeta not found, saving without extended metadata."
            local is_nolabel 1
        }
    }
    
    // Call Python
    python: import dtparquet
    python: dtparquet.save("`filename'", bool(`is_nolabel'))
    
    // Cleanup metadata frames
    if `is_nolabel' == 0 {
        foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
            capture frame drop `fr'
        }
    }
end

program dtparquet_use
    // Manual parsing to handle [varlist] [if] [in] using filename [, clear nolabel]
    // because syntax [if] validates against memory and syntax [anything] 
    // often chokes on 'using'.
    
    local cmdline `"`0'"'
    
    // 1. Separate options
    gettoken everything options : cmdline, parse(",")
    if substr(`"`options'"', 1, 1) == "," local options = substr(`"`options'"', 2, .)
    
    local is_nolabel = strpos(`"`options'"', "nolabel") > 0
    local is_clear = strpos(`"`options'"', "clear") > 0
    
    // 2. Find 'using' in 'everything'
    local upos = strpos(`"`everything'"', " using ")
    if `upos' == 0 {
        if substr(trim(`"`everything'"'), 1, 5) == "using" {
            local prefix ""
            local remainder `"`everything'"'
        }
        else {
            // No 'using' found. Assume: filename [if] [in]
            gettoken filename if_in : everything
            local prefix ""
            local remainder ""
        }
    }
    else {
        local prefix = substr(`"`everything'"', 1, `upos'-1)
        local remainder = substr(`"`everything'"', `upos'+1, .)
    }
    
    // 3. Parse filename from remainder
    if `"`remainder'"' != "" {
        gettoken using_kw remainder : remainder // eat 'using'
        gettoken filename if_in_after : remainder
    }
    
    if `"`filename'"' == "" {
        di as error "using required"
        exit 100
    }
    
    // 4. Parse varlist and if/in from prefix
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
    
    // 5. Validation & Cleanup
    local filename = subinstr(trim(`"`filename'"'), `"""', "", .)
    local filename : subinstr local filename "\" "/", all
    
    if `is_clear' == 0 & (c(N) > 0 | c(k) > 0) {
        error 4
    }
    
    // 6. Call Python
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
    else {
        local py_varlist "None"
    }
    
    python: dtparquet.load("`filename'", `py_varlist', bool(`is_nolabel'))
    
    // 7. Apply if/in if specified
    if `"`if_in'"' != "" {
        quietly keep `if_in'
    }
    
    // 8. Restore metadata
    if `is_nolabel' == 0 {
        _apply_dtmeta
    }
end




program _apply_dtmeta
    // Restore variable labels and formats from _dtvars
    capture frame _dtvars: count
    if _rc == 0 {
        local nvars = r(N)
        forvalues i = 1/`nvars' {
            frame _dtvars: local vname = varname[`i']
            
            // Check if variable exists in current frame
            capture confirm variable `vname'
            if _rc == 0 {
                frame _dtvars: local vlab = varlab[`i']
                frame _dtvars: local vfmt = format[`i']
                frame _dtvars: local vlbl = vallab[`i']
                
                if `"`vlab'"' != "" label variable `vname' `"`vlab'"'
                if `"`vfmt'"' != "" format `vname' `vfmt'
                if `"`vlbl'"' != "" {
                    capture label values `vname' `vlbl'
                }
            }
        }
    }
    
    // Restore value labels from _dtlabel
    capture frame _dtlabel: count
    if _rc == 0 {
        frame _dtlabel: levelsof vallab, local(lablist)
        foreach lbl of local lablist {
            // Collect value/label pairs for this label name
            tempname curlblfr
            frame copy _dtlabel `curlblfr'
            frame `curlblfr': keep if vallab == "`lbl'"
            frame `curlblfr': count
            local nlbls = r(N)
            forvalues j = 1/`nlbls' {
                frame `curlblfr': local val = value[`j']
                frame `curlblfr': local txt = label[`j']
                label define `lbl' `val' `"`txt'"', add
            }
            frame drop `curlblfr'
        }
    }
    
    // Restore variable notes from _dtnotes
    capture frame _dtnotes: count
    if _rc == 0 {
        local nnotes = r(N)
        forvalues i = 1/`nnotes' {
            frame _dtnotes: local vname = varname[`i']
            capture confirm variable `vname'
            if _rc == 0 {
                frame _dtnotes: local vnote = _note_text[`i']
                notes `vname': `"`vnote'"'
            }
        }
    }
    
    // Restore dataset info from _dtinfo
    capture frame _dtinfo: count
    if _rc == 0 {
        frame _dtinfo: local dlab = dta_label[1]
        if `"`dlab'"' != "" label data `"`dlab'"'
        
        // Dataset notes
        local dnotes = 0
        capture frame _dtinfo: count if !missing(dta_note)
        if _rc == 0 local dnotes = r(N)
        forvalues i = 1/`dnotes' {
            frame _dtinfo: local dnote = dta_note[`i']
            notes: `"`dnote'"'
        }
    }
    
    // Cleanup
    foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
        capture frame drop `fr'
    }
end
