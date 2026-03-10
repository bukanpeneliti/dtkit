*! Version 1.0.2 25jun2025
program define dtfreq
    * Module to produce frequency dataset

    version 16
    syntax anything(id="varlist") [if] [in] [aweight fweight iweight pweight] [using/] [, df(string) by(varname numeric) cross(varname numeric) BINary FOrmat(string) noMISS save(string asis) excel(string) STATs(namelist max=3) TYpe(namelist max=2) Clear REPlace]

    // Validate arguments and get returned parameters
    _argload, clear(`clear') using(`using')
    // Define frames
    local source_frame `r(source_frame)'
    local _defaultframe `r(_defaultframe)'

    // Now validate the varlist as numeric with loaded data
    local varlist `anything'

    _argcheck `varlist' `if' `in' [`weight'`exp'], df(`df') by(`by') cross(`cross') `binary' format(`format') `miss' using(`using') excel(`excel') stats("`stats'") type("`type'") save(`save') replace(`replace') clear(`clear')

    local fullname "`r(fullname)'"

    // * Set defaults
    if "`df'" == "" local df "_df"
    if "`stats'" == "" local stats "col"
    if "`type'" == "" local type "prop"

    capture frame drop `df'
    frame create `df'
    tempname temp_frame
    frame create `temp_frame'

    // * weight and marker
    tempvar touse
    if "`miss'" == "nomiss" {
        marksample touse, strok
    }
    else {
        marksample touse, strok novarlist
    }
    local ifcmd "if `touse'"
    if "`weight'" != "" {
        local wtexp `"[`weight'`exp']"'
    }

    // tabulation
    _xtab `varlist', ifcmd(`ifcmd') wtexp(`wtexp') df(`df') by(`by') cross(`cross') binary(`binary') source_frame(`source_frame') temp_frame(`temp_frame')

    // give labels
    _labelvars, df(`df') by(`by') cross(`cross') source_frame(`source_frame') binary(`binary') ifcmd(`ifcmd')

    // format vars
    frame `df': quietly ds *
    if "`format'" == "" {
        frame `df': _formatvars `r(varlist)'
    }
    else {
        frame `df': quietly ds *, has(type numeric)
        frame `df': format `r(varlist)' `format' 
    }

    // add total
    if "`cross'" != "" & "`binary'" == "" {
        frame `df': rename (colprop_ colpct_) (prop_all pct_all)
        frame `df': _crosstotal
    }
        
    // drop variables
    local core_vars "`by' varname varlab"
    if "`binary'" == "" local core_vars "`core_vars' vallab"
    if "`cross'" != "" frame `df': order `core_vars' *prop* *pct* freq* rowfreq* total* 
    else frame `df': order `core_vars' *prop* *pct* freq* total* 
    if strpos("`stats'", "row") == 0 frame `df': capture drop row*
    if strpos("`stats'", "col") == 0 frame `df': capture drop col*
    if strpos("`stats'", "cell") == 0 frame `df': capture drop cell*
    if strpos("`type'", "prop") == 0 frame `df': capture drop *prop*
    if strpos("`type'", "pct") == 0 frame `df': capture drop *pct*

    // sort results (freq always exists)
    frame `df': sort `by' varname freq*

    // export to excel
    if `"`save'"' != "" {
        frame `df': _toexcel, fullname("`fullname'") excel(`excel') replace(`replace')
    }

end

// * Loops through variables and groups to make tables
program define _xtab
    syntax varlist(min=1 numeric) [, df(name) by(name) cross(name) binary(name) source_frame(name) temp_frame(name) ifcmd(string) wtexp(string)]

    foreach var of local varlist {

        // Get variable and value label from main data
        local `var'_varlab: variable label `var'
        if "``var'_varlab'" == "" local `var'_varlab "`var'"
        
        // If by specified, process each level separately  
        if "`by'" != "" {
            frame `source_frame': quietly levelsof `by' `ifcmd', local(by_levels)
            
            foreach level in `by_levels' {
                // Get by label from main frame
                frame `source_frame': local by_label: label (`by') `level'
                if "`by_label'" == "" local by_label "`level'"
                
                // Run analysis for this level
                frame `temp_frame' {
                    _xtab_core, var(`var') by(`by') cross(`cross') ///
                        varlab(``var'_varlab') level(`level') ///
                        source_frame(`source_frame') binary(`binary') by_condition("& `by' == `level'") ///
                        ifcmd(`ifcmd') wtexp(`wtexp')
                    tempfile _result
                    quietly save `_result'
                }
                frame `df': quietly append using `_result'
            }
            
            // Add totals (all data)
            frame `temp_frame' {
                _xtab_core, var(`var') cross(`cross') varlab(``var'_varlab') ///
                    level(-1) binary(`binary') source_frame(`source_frame') ///
                    ifcmd(`ifcmd') wtexp(`wtexp') by(`by')
                tempfile _result
                quietly save `_result'
            }
            frame `df': quietly append using `_result'
            
        }
        else {
            // No by - just run once
            frame `temp_frame' {
                _xtab_core, var(`var') cross(`cross') varlab(``var'_varlab') ///
                    level(-1) binary(`binary') source_frame(`source_frame') ///
                    ifcmd(`ifcmd') wtexp(`wtexp')
                tempfile _result
                quietly save `_result'
            }
            frame `df': quietly append using `_result'
        }

    }

end

// * Does the actual counting and math for each table
program define _xtab_core
    // use name instead of varname
    syntax, var(name) varlab(string) level(real) source_frame(name) ///
        [by(name) cross(name) binary(name) by_condition(string) ifcmd(string) wtexp(string)]

    frame `source_frame': quietly levelsof `var' `ifcmd' `by_condition', local(vallabels)
    // Create tabulation with if condition
    if "`cross'" != "" local tabcmd "quietly tabulate `var' `cross' `wtexp' `ifcmd' `by_condition', matcell(_FREQ) matrow(_ROWVAL) matcol(_COLVAL)" // Two-way tabulation
    else local tabcmd "quietly tabulate `var' `wtexp' `ifcmd' `by_condition', matcell(_FREQ) matrow(_ROWVAL)" // One-way tabulation 
    frame `source_frame': `tabcmd'
    
    // Call mata function
    mata: _xtab_core_calc()

    // Build variable names - match matrix structure
    local varnamelist "numlab"
    
    if "`cross'" != "" {
        foreach prefix in freq col row cell {
            foreach col in `colval' {
                local varnamelist `varnamelist' `prefix'prop`col'
            }
        }
        local varnamelist = subinstr("`varnamelist'", "freqprop", "freq", .)
    }
    else local varnamelist "`varnamelist' freq prop" // One-way: simpler structure

    // Add row values and set column names
    matrix _FULLMAT = (_ROWVAL, _FULLMAT)
    matrix colnames _FULLMAT = `varnamelist'
    
    // Create results dataset
    clear
    quietly {
        svmat _FULLMAT, names(col)
        
        generate varname = "`var'", before(numlab)
        generate varlab = "`varlab'", before(numlab)
        capture generate `by' = `level', before(numlab)
        generate vallab = "", before(numlab)
    }

    // Fill value labels using main frame
    foreach val in `vallabels' {
        frame `source_frame': local vallabval: label (`var') `val'
        if "`vallabval'" == "" local vallabval "`val'"
        quietly replace vallab = "`vallabval'" if numlab == `val'
    }

    // calculate percentage
    quietly ds *prop*, has(type numeric)
    foreach var in `r(varlist)' {
        local pctname: subinstr local var "prop" "pct"
        quietly generate `pctname' = `var' * 100
    }
    
    // Calculate totals
    if "`cross'" != "" {
        // Two-way - use actual cross variable values from the column values
        // The colval should contain the cross variable levels from mata
        foreach val in `colval' {
            capture egen total`val' = total(freq`val')
        }
        egen rowfreq = rowtotal(freq*), missing
        egen total_all = total(rowfreq)
        generate colprop_ = rowfreq / total_all
        generate colpct_ = colprop_ * 100
    }
    else {
        // One-way
        egen total = total(freq)
    }

    drop numlab
    if "`binary'" != "" _binreshape, by(`by') cross(`cross')
end

// * reshape binary data (formerly yesno)
program define _binreshape
    syntax, [by(name) cross(name)]
    
    quietly replace vallab = strlower(subinstr(vallab, " ", "_", .))
    quietly levelsof vallab, local(vallab_value)
    quietly replace vallab = "_" + strlower(vallab)

    // Determine which variables to reshape based on cross
    if "`cross'" != "" quietly ds freq* col* row* cell*, has(type numeric)
    else quietly ds freq prop, has(type numeric)

    local reshape_vars `r(varlist)'

    // Store variable labels before reshape
    foreach var in `reshape_vars' {
        local `var'_label: variable label `var'
    }

    if "`cross'" == "" {
        quietly reshape wide freq prop pct, i(`by' varname varlab) j(vallab) string
    }
    if "`cross'" != "" {
        quietly ds *, has(type numeric)
        foreach numvar in `r(varlist)' {
            local `numvar'_varlab: variable label `numvar'
        }
        quietly reshape wide freq* col* row* cell*, i(`by' varname varlab) j(vallab) string
    }

end

// * adds total row for cross option
program define _crosstotal
    syntax, [vallabname(name)] // Optional vallab variable name

    // Handle optional vallab (default to 'vallab' if not specified)
    if "`vallabname'" == "" {
        local vallabname "vallab"
        capture confirm variable vallab
        if _rc {
            di as text "Note: Creating missing 'vallab' variable"
            quietly generate vallab = ""
        }
    }

    // Identify key variables
    unab freqvars: freq*          // Frequency variables (freq1, freq2, ...)
    unab totalvars: total*        // Total variables (total1, ..., total_all)
    local rowfreq rowfreq         // Row frequency variable

    // Preserve original totals and labels
    quietly {
        preserve
            keep varname varlab `vallabname' `totalvars'
            duplicates drop varname, force
            tempfile totals
            save `totals'
        restore

        // Create total rows
        preserve
            collapse (sum) `freqvars' `rowfreq' , by(varname varlab)
            merge 1:1 varname using `totals', nogen

            // Set category label to "Total"
            replace `vallabname' = "Total"

            // Calculate proportions
            foreach tvar of local totalvars {
                if "`tvar'" != "total_all" {
                    local suffix = substr("`tvar'", 6, .)
                    generate cellprop`suffix' = `tvar' / total_all
                    generate rowprop`suffix' = cellprop`suffix'  // Same as cellprop in totals
                    generate colprop`suffix' = 1
                    generate cellpct`suffix' = cellprop`suffix' * 100
                    generate rowpct`suffix' = rowprop`suffix' * 100
                    generate colpct`suffix' = colprop`suffix' * 100
                    replace freq`suffix' = `tvar'   // Set freq to column total
                }
            }
            generate prop_all = 1
            generate pct_all = 100
            replace `rowfreq' = total_all  // Set row frequency to overall total
            tempfile totalrows
            save `totalrows'
        restore

        // Append and sort
        append using `totalrows'
        generate sortorder = 0
        replace sortorder = 1 if `vallabname' == "Total"
        sort varname sortorder
        drop sortorder
        replace varlab = "Grand total" if `vallabname' == "Total"
    }
end
// * Adds nice names to all output columns
program define _labelvars
    syntax, [df(name) by(name) cross(name) source_frame(name) binary(name) ifcmd(string)]
    // standard vars/vars in one-way
    frame `df' {
        label variable varname "Variable"
        label variable varlab "Variable label"
        capture label variable vallab "Value"
        capture label variable freq "Frequency"
        capture label variable prop "Proportion"
        capture label variable total "Total"
    }

    // by specified
    if "`by'" != "" {
        frame `source_frame': local by_varlab: variable label `by' 
        frame `source_frame': local byvallab: value label `by'
        frame `df' {
            label variable `by' "`by_varlab'"
            frame `source_frame': quietly levelsof `by' `ifcmd', local(by_levels)
            local labupper = strupper("`by'")
            foreach level in `by_levels' {
                frame `source_frame': local vallabtext: label (`by') `level'
                label define `labupper' `level' "`vallabtext'", modify
            }
            label define `labupper' -1 "Total", modify
            label values `by' `labupper'
        } 
    }
    // cross specified
    if "`binary'" == "" & "`cross'" != "" {
        frame `source_frame': quietly levelsof `cross', local(cross_values)
        foreach val in `cross_values' {
            frame `source_frame': local cross_lbl_`val': label (`cross') `val'            
            frame `df': label variable freq`val' "Frequency `cross_lbl_`val''"
            frame `df': label variable total`val' "Total `cross_lbl_`val''"
            frame `df': label variable rowprop`val' "Row proportion `cross_lbl_`val''"
            frame `df': label variable colprop`val' "Column proportion `cross_lbl_`val''"
            frame `df': label variable cellprop`val' "Cell proportion `cross_lbl_`val''"
            frame `df': label variable rowpct`val' "Row percentage (%) `cross_lbl_`val''"
            frame `df': label variable colpct`val' "Column percentage (%) `cross_lbl_`val''"
            frame `df': label variable cellpct`val' "Cell percentage (%) `cross_lbl_`val''"
            frame `df': label variable colprop_ "Overall column proportion"
            frame `df': label variable colpct_ "Overall column percentage (%)"
        }
        frame `df': label variable rowfreq "Overall row frequency"
        frame `df': label variable total_all "Overall total count"
    }
    else if "`binary'" != "" & "`cross'" == "" {
        frame `df': quietly ds freq* prop*
        foreach reshapevars in `r(varlist)' {
            frame `df': local `reshapevars'_varlab: variable label `reshapevars'
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "_" "["
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab " " "] "
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "freq" "Frequency"
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "prop" "Proportion"
            frame `df': label variable `reshapevars' "``reshapevars'_varlab'"
        }
    }
    else if "`binary'" != "" & "`cross'" != "" {
        // get value and variable label from cross
        frame `source_frame': quietly levelsof `cross', local(cross_values)
        frame `df': quietly ds freq* col* row* cell*
        foreach reshapevars in `r(varlist)' {
            frame `df': local `reshapevars'_varlab: variable label `reshapevars'
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "_" "["
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab " " "] "
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "_" " ", all
            foreach val in `cross_values' {
                frame `source_frame': local cross_lbl_`val': label (`cross') `val'
                local cross_lbl_`val' = strlower("`cross_lbl_`val''")
                local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "`val'" " `cross_lbl_`val''"
                frame `df': label variable total`val' "Total `cross_lbl_`val''"
            }
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "freq" "Frequency | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "colprop" "Column proportion | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "rowprop" "Row proportion | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "cellprop" "Cell proportion | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "colpct" "Column percentage (%) | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "rowpct" "Row percentage (%) | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "cellpct" "Cell percentage (%) | ", word
            local `reshapevars'_varlab: subinstr local `reshapevars'_varlab "rowfreq" "Row frequency"
            frame `df': label variable `reshapevars' "``reshapevars'_varlab'"
        }
        frame `df': label variable total_all "Overall total count"
    }
    frame `df': order total*, last
end

// * Saves the final table to Excel file
program define _toexcel

    syntax, [fullname(string asis) excel(string) replace(string)]

    if "`replace'" == "" local replace "modify"
    if `"`fullname'"' != "" {
        // Set export options
        if `"`excel'"' == "" {
            local exportcmd `"`fullname', sheet("dtfreq_output", `replace') firstrow(varlabels)"'
        }
        else {
            local exportcmd `"`fullname', `excel'"'
        }
        
        // Perform export with error handling
        export excel using `exportcmd'
    }

end

// * Makes numbers look good with commas and decimals
program define _formatvars
    syntax varlist, [report]
    foreach var of local varlist {
        // skip string vars
        local vartype: type `var'
        if substr("`vartype'",1,3) == "str" {
            local varformat: format `var'
            local varformat: subinstr local varformat "%" "%-"
            format `var' `varformat'
            continue
        }

        * Check if variable has date-time format and skip if so
        local current_fmt : format `var'
        if regexm("`current_fmt'", "^%t[cCdwmqhy].*") | regexm("`current_fmt'", "^%d.*") {
            if "`report'" != "" display "Variable `var': Skipping date-time format (`current_fmt')"
            continue
        }
        
        * Check if variable has value labels - if yes, just add negative sign and continue
        local vallbl : value label `var'
        if "`vallbl'" != "" {
            local current_format : format `var'
            local new_format : subinstr local current_format "%" "%-"
            format `var' `new_format'
            if "`report'" != "" display "Variable `var': Has value labels, applying left-justification (`new_format')"
            continue
        }
        
        * Get summary statistics
        quietly summarize `var', meanonly
        local max_val = r(max)
        local min_val = r(min)
                
        * Check if variable has decimal parts
        capture assert `var' == round(`var') if !missing(`var')
        local has_decimals = (_rc == 9)
        
        * Determine format based on rules
        local format_str ""
        
        * Rule 2: Proportions (0 to 1 range) - 3 decimal places
        if `max_val' <= 1 & `min_val' >= 0 {
            local width = 5 + 2  // "0.123" = 5 characters + 2 buffer
            local format_str "%`width'.3f"
            if "`report'" != "" display "Variable `var': Detected as proportion, using format `format_str'"
        }
        
        * Rule 1 & 3: Large numbers (>=1000)
        else if `max_val' >= 1000 & !missing(`max_val') {
            * Calculate width needed for largest number
            local max_digits = floor(log10(`max_val')) + 1
            local commas = floor((`max_digits' - 1) / 3)
            
            if `has_decimals' {
                * Rule 1 + 3: Large numbers with decimals (1 decimal place + comma)
                local width = `max_digits' + `commas' + 2 + 2  // +2 for ".X", +2 buffer
                local format_str "%`width'.1fc"
                if "`report'" != "" display "Variable `var': Large number with decimals, using format `format_str'"
            }
            else {
                * Rule 1: Large integers (no decimal places + comma)
                local width = `max_digits' + `commas' + 2  // +2 buffer
                local format_str "%`width'.0fc"
                if "`report'" != "" display "Variable `var': Large integer, using format `format_str'"
            }
        }
        
        * Smaller numbers (<1000)
        else {
            local max_digits = max(1, floor(log10(max(abs(`max_val'), abs(`min_val')))) + 1)
            
            if `has_decimals' {
                * Small numbers with decimals (1 decimal place, no comma)
                local width = `max_digits' + 2 + 2  // +2 for ".X", +2 buffer
                local format_str "%`width'.1f"
                if "`report'" != "" display "Variable `var': Small number with decimals, using format `format_str'"
            }
            else {
                * Small integers (no decimal places, no comma)
                local width = `max_digits' + 2  // +2 buffer
                local format_str "%`width'.0f"
                if "`report'" != "" display "Variable `var': Small integer, using format `format_str'"
            }
        }
        
        * Apply the format
        format `var' `format_str'
    }
end

// * Checks if user inputs are valid before starting
program define _argcheck, rclass
    syntax varlist(min=1 numeric) [if] [in] [aweight fweight iweight pweight] ///
           [, df(string) by(varname numeric) cross(varname numeric) BINary ///
           FOrmat(string) noMISS using(string) stats(namelist) type(namelist) ///
           fullpath(string) filename(string) extension(string) replace(string) excel(string) save(string asis) clear(string)]

    // * Validate stats and type options
    if "`stats'" != "" {
        local dupstats: list dups stats
        if "`dupstats'" != "" {
            display as error "Option stats() must be unique. Duplicates found: " as result "`dupstats'" as error " in " as result "stats(`stats')" as error "."
            exit 198
        }
        // Check if all elements are valid
        local valid_stats "row col cell"
        local invalid_stats: list stats - valid_stats
        if "`invalid_stats'" != "" {
            display as error "Invalid stats option(s): " as result "`invalid_stats'" as error ". Valid options are: row, col, or cell (without commas)."
            exit 198
        }
        // Check maximum of 3 elements
        local stats_count: word count `stats'
        if `stats_count' > 3 {
            display as error "Option stats() allows maximum 3 values. You specified `stats_count': " as result "`stats'"
            exit 198
        }
    }
    if "`type'" != "" {
        local duptype: list dups type
        if "`duptype'" != "" {
            display as error "Option type() must be unique. Duplicates found: " as result "`duptype'" as error " in " as result "type(`type')" as error "."
            exit 198
        }
        // Check if all elements are valid
        local valid_types "prop pct"
        local invalid_types: list type - valid_types
        if "`invalid_types'" != "" {
            display as error "Invalid type option(s): " as result "`invalid_types'" as error ". Valid options are: prop, pct"
            exit 198
        }
        
        // Check maximum of 2 elements
        local type_count: word count `type'
        if `type_count' > 2 {
            display as error "Option type() allows maximum 2 values. You specified `type_count': " as result "`type'"
            exit 198
        }
    }

    // * Cross-option validation
    // clear only makes sense together with using
    if "`clear'" != "" & "`using'" == "" {
        display as error "option clear only allowed with using"
        exit 198
    }

    // replace only makes sense together with save
    if "`replace'" != "" & `"`save'"' == "" {
        display as error "option replace only allowed with save"
        exit 198
    }

    // Ensure excel is only present if using is present
    if `"`save'"' == "" & "`excel'" != "" {
        display as error "excel() option is only allowed when save() is also specified."
        exit 198
    }

    // Issue warning if binary and cross are both used
    if "`binary'" != "" & "`cross'" != "" {
        display as text "Note: binary option with cross() may produce complex output structure."
    }

    // Ensure by and cross are not the same if both are specified
    if "`by'" != "" & "`cross'" != "" & "`by'" == "`cross'" {
        display as error "by() variable and cross() variable cannot be the same."
        exit 198
    }

    // ensure stats can only be used with cross
    if "`stats'" != "" & "`cross'" == "" {
        display as error "stats() option is only allowed when cross() is also specified."
        exit 198
    }


    // * Binary option validation (domain-specific)
    if "`binary'" != "" {
        tempname _chklbl
        frame put `varlist', into(`_chklbl')
        frame `_chklbl' {
            foreach var of local varlist {
                // Check if value label exists
                local lbl : value label `var'
                if "`lbl'" == "" {
                    // Create temporary value label
                    tempname tmplbl
                    qui levelsof `var', local(values)
                    foreach val in `values' {
                        local lbltxt = strofreal(`val') // Convert number to string
                        label define `tmplbl' `val' "`lbltxt'", add
                    }
                    label values `var' `tmplbl'
                    if "`debug'" == "1" {
                        di as text "Temporary label applied: `var'"
                    }
                }
            }

            quietly uselabel, clear var
            ren lname labelname
            quietly generate varname = ""
            foreach lbl in `r(__labnames__)' {
                quietly replace varname = "`r(`lbl')'" if labelname == "`lbl'"
            }
            quietly sort labelname value
            quietly by labelname: generate index = _n
            quietly egen indexmax = max(index), by(labelname)
            quietly levelsof index, local(levels)
            if `r(r)' != 2 {
                display as error "Binary option only allow exactly two values per variable. The following label has more or less than 2."
                list varname value label if indexmax != 2, sepby(labelname)
                exit 198
            }
                
            quietly reshape wide value label trunc, i(labelname varname) j(index)
            egen grup = group(value* label*), missing
            sort grup, stable
            quietly levelsof grup, local(grupvals)
            if `r(r)' > 1 {
                display as error "The following variables have inconsistent values/labels:"
                list varname labelname value* label*, sepby(grup) noobs subvarname
                exit 198
            }
        }
    }

    // * Excel export
    if `"`save'"' != "" {
        local inputfile = subinstr(`"`save'"', `"""', "", .)
        
        // Validate against path traversal attacks
        if ustrregexm("`inputfile'", "\.\./") {
            display as error "Path traversal attempts are not allowed in save() option"
            exit 198
        }
        
        if ustrregexm("`inputfile'", "^(.*[/\\])?([^/\\]+?)(\.[^./\\]+)?$") {
            local dir_part = ustrregexs(1)
            local filename = ustrregexs(2)
            local extension = ustrregexs(3)
            if "`extension'" == "" local extension = ".xlsx"
            
            // Handle directory part properly
            if "`dir_part'" == "" {
                local fullpath = c(pwd)
            }
            else {
                // Use pathutil for cross-platform path handling
                local fullpath = "`dir_part'"
                if !ustrregexm("`fullpath'", "^[A-Za-z]:") & !ustrregexm("`fullpath'", "^[/\\]") {
                    // Relative path - make it absolute
                    local fullpath = c(pwd) + c(dirsep) + "`dir_part'"
                }
            }
            
            local fullname = "`fullpath'" + c(dirsep) + "`filename'`extension'"
            
            // Test directory accessibility
            local workdir = c(pwd)
            capture cd "`fullpath'"
            if _rc == 170 {
                display as error "Cannot access the directory specified in save() option: " as result "`fullpath'"
                exit 601
            }
            else {
                quietly cd "`workdir'"
                return local fullpath "`fullpath'"
                return local filename "`filename'"
                return local extension "`extension'"
                return local fullname "`fullname'"
                return local save `"`save'"'
                return local excel "`excel'"
                return local replace "`replace'"
            }
        }
        else {
            display as error "Invalid file path format in save() option"
            exit 198
        }
    }
end

// * Determines the data source
program define _argload, rclass
    syntax, [using(string) clear(string)]

    local _inmemory = c(filename) != "" | c(N) > 0 | c(k) > 0 | c(changed) == 1
    if `_inmemory' == 0 & "`using'" == "" {
        display as error "No data source for executing dtfreq. Please specify a dataset using the 'using' or load the data into memory."
        exit 198
    }

    // define dataset
    if "`using'" != "" {
        if `_inmemory' == 1 & "`clear'" == "" {
            return local _defaultframe = c(frame)
            capture frame drop _dtsource
            frame create _dtsource
            cwf _dtsource
            return local source_frame "_dtsource"
            quietly use `"`using'"', clear
        }
        else {
            quietly use `"`using'"', clear
            return local source_frame = c(frame)
        }
    }
    else if "`using'" == "" & `_inmemory' == 1 return local source_frame = c(frame)

end

// * Calculates percentages and proportions in Mata
mata:
void _xtab_core_calc()
{
    _FREQ = st_matrix("_FREQ")
    _ROWVAL = st_matrix("_ROWVAL")
    
    // Check if this is one-way or two-way
    if (st_local("cross") == "") {
        // One-way tabulation
        _PROP = _FREQ / sum(_FREQ)
        _FULLMAT = (_FREQ, _PROP)
        st_matrix("_FULLMAT", _FULLMAT)
        st_local("colval", "")  // No column values for one-way
    }
    else {
        // Two-way tabulation (existing logic)
        _COLVAL = st_matrix("_COLVAL")
        
        rowsum = _FREQ * J(cols(_FREQ),1,1)
        colsum = J(1,rows(_FREQ),1) * _FREQ
        
        _COLPROP = _FREQ :/ (J(rows(_FREQ),1,1) * colsum)
        _ROWPROP = _FREQ :/ (rowsum * J(1,cols(_FREQ),1))
        _CELLPROP = _FREQ / sum(_FREQ)
        
        _FULLMAT = (_FREQ, _COLPROP, _ROWPROP, _CELLPROP)
        st_matrix("_FULLMAT", _FULLMAT)
        st_local("colval", invtokens(strofreal(_COLVAL)))
    }
}
end