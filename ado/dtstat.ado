*! Version 1.0.2 25jun2025
program define dtstat
    * Module to produce descriptive statistics dataset

    version 16
    syntax anything(id="varlist") [if] [in] [aweight fweight iweight pweight] [using/] [, df(string) by(varlist) stats(string asis) FOrmat(string) noMISS FAst save(string asis) excel(string) REPlace Clear]

    // Validate arguments and get returned parameters
    _argload, clear(`clear') using(`using')
    // Define frames
    local source_frame `r(source_frame)'
    local _defaultframe `r(_defaultframe)'

    // Now validate the varlist as numeric with loaded data
    local varlist `anything'

    // Initialize and validate inputs
    _argcheck, fast(`fast') save(`save') excel(`excel') replace(`replace') varlist(`varlist')
    local collapsecmd "`r(collapsecmd)'"
    local fullname "`r(fullname)'"

    // * Set defaults
    if "`df'" == "" local df "_df"
    capture frame drop `df'
    frame create `df'
    tempname temp_frame
    frame create `temp_frame'

    // * weight and marker
    tempvar touse
    if "`miss'" == "nomiss" marksample touse, strok
    else marksample touse, strok novarlist
    local ifcmd "if `touse'"
    if "`weight'" != "" local wtexp `"[`weight'`exp']"'

    // Process statistics options
    _stats, stats(`stats')    
    local stats_list "`r(stats_list)'"
    local total_id "`r(total_id)'"

    // Perform main collapse operations
    _collapsevars `varlist', by(`by') ifcmd(`ifcmd') wtexp(`wtexp') collapsecmd(`collapsecmd') ///
        df(`df') stats_list(`stats_list') total_id(`total_id') ///
        temp_frame(`temp_frame') source_frame(`source_frame')

    // Apply formatting and labels
    _format, by(`by') format(`format') df(`df')    

    // export to excel
    if `"`save'"' != "" {
        frame `df': _toexcel, fullname("`fullname'") excel(`excel') replace(`replace')
    }
end

// * statistics processing
program define _stats, rclass
    syntax, [stats(string asis)]
    
    // Default statistics if STATS() option is not specified
    if `"`stats'"' == "" {
        local stats "count mean median min max"
    }
    
    // Define the total identifier value
    local total_id -1 // Using -1 to represent totals
    return local total_id "`total_id'"
    return local stats_list "`stats'"
    
end

// * main collapse loop
program define _collapsevars
    syntax varlist, [by(string) ifcmd(string) wtexp(string) collapsecmd(string) ///
        collapse_stats(string asis) df(string) stats_list(string) total_id(string) ///
        temp_frame(name) source_frame(name)]
    
    local varcount = 1
    frame `source_frame' {
        foreach var in `varlist' {
            local `var'lab: variable label `var'

            // Build collapse syntax for this specific variable
            local var_collapse_stats ""
            foreach vartype in `stats_list' {
                local var_collapse_stats `"`var_collapse_stats' (`vartype') `vartype'=`var'"'
            }
            frame copy `source_frame' `temp_frame', replace
            
            // option 1: without by
            frame `temp_frame' {
                if "`by'" == "" {
                    capture `collapsecmd' `var_collapse_stats' `wtexp' `ifcmd', fast favor(speed)
                    if _rc != 0 `collapsecmd' `var_collapse_stats' `wtexp' `ifcmd', fast
                }
                // option 2: with by
                else if "`by'" != "" {
                    _byprocess, by(`by') collapsecmd(`collapsecmd') ///
                        collapse_stats(`var_collapse_stats') ifcmd(`ifcmd') wtexp(`wtexp') ///
                        varcount(`varcount') total_id(`total_id')
                }

                quietly generate varname = "`var'"
                quietly generate varlab = "``var'lab'", after(varname)

                tempfile data`var'
                quietly save `data`var''
                frame `df': quietly append using `data`var''
            }
            local ++varcount
        }
    }
end

// * by-group processing
program define _byprocess
    syntax, [by(string) collapsecmd(string) collapse_stats(string asis) ///
        ifcmd(string) wtexp(string) varcount(string) total_id(string)]
    
    if "`by'" != "" {
        tempvar expanded
        quietly expand 2, generate(`expanded')
        
        foreach byvar in `by' {
            // Check if total_id already exists in by values
            quietly levelsof `byvar', local(existing_vals)
            if `: list posof "`total_id'" in existing_vals' > 0 {
                display as error "Value `total_id' already exists in `byvar'. Cannot create total row."
                exit 198
            }

            quietly replace `byvar' = `total_id' if `expanded' == 1

            // Handle value labels for row totals
            local by_vallbl : value label `byvar'
            if "`by_vallbl'" == "" {
                tempname by_temp_lbl
                label define `by_temp_lbl' `total_id' "Total", modify
                label values `byvar' `by_temp_lbl'
                if `varcount' == 1 {
                    display as text "Note: Added temporary labels to `byvar' for totals"
                }
            }
            else {
                capture label define `by_vallbl' `total_id' "Total", modify
                if _rc != 0 {
                    display as error "Cannot modify existing value labels for `byvar'"
                    exit 198
                }
            }
        }
        
        capture `collapsecmd' `collapse_stats' `wtexp' `ifcmd', by(`by') fast favor(speed)
        if _rc != 0 `collapsecmd' `collapse_stats' `wtexp' `ifcmd', by(`by') fast
        quietly for var `by': drop if missing(X)

    }
end

// * apply formatting and labeling
program define _format
    syntax, [by(string) format(string)] df(string)
    
    frame `df' {
        order `by' varname varlab
        sort `by' varname varlab, stable

        quietly replace varlab = substr(varlab,strpos(varlab, ".") + 2, strlen(varlab)) if strpos(varlab, ".")>0

        // Apply variable labels
        _labelvars
        
        // Apply formatting - respect user format choice
        quietly describe, varlist
        local all_vars "`r(varlist)'"
        
        if "`format'" == "" {
            _formatvars `all_vars'
        }
        else {
            quietly ds *, has(type numeric)
            format `r(varlist)' `format'
        }
    }
end

// * make variable labels
program define _labelvars
    capture label variable mean "means"
    capture label variable median "medians"

    forvalues i = 1/99 {
        local suffix "th"
        if inrange(`i', 11, 13) local suffix "th"
        else if mod(`i', 10) == 1 local suffix "st"
        else if mod(`i', 10) == 2 local suffix "nd"
        else if mod(`i', 10) == 3 local suffix "rd"
        capture label variable p`i' "`i'`suffix' percentile"
    }

    capture label variable sd "standard deviations"
    capture label variable semean "standard error of the mean (sd/sqrt(n))"
    capture label variable sebinomial "standard error of the mean, binomial (sqrt(p(1-p)/n))"
    capture label variable sepoisson "standard error of the mean, Poisson (sqrt(mean/n))"
    capture label variable sum "sums"
    capture label variable rawsum "sums, ignoring optionally specified weight except observations with a weight of zero are excluded"
    capture label variable count "number of nonmissing observations"
    capture label variable percent "percentage of nonmissing observations in the by group"
    capture label variable max "maximums"
    capture label variable min "minimums"
    capture label variable iqr "interquartile range"
    capture label variable first "first value"
    capture label variable last "last value"
    capture label variable firstnm "first nonmissing value"
    capture label variable lastnm "last nonmissing value"
    label variable varname "Variable"
    label variable varlab "Variable label"
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

// * Saves the final table to Excel file
program define _toexcel

    syntax, [fullname(string asis) excel(string) replace(string)]

    if "`replace'" == "" local replace "modify"
    if `"`fullname'"' != "" {
        // Set export options
        if `"`excel'"' == "" {
            local exportcmd `"`fullname', sheet("dtstat_output", `replace') firstrow(varlabels)"'
        }
        else {
            local exportcmd `"`fullname', `excel'"'
        }
        
        // Perform export with error handling
        export excel using `exportcmd'
    }

end

// * Checks if user inputs are valid before starting
program define _argcheck, rclass
    syntax, [fast(string) excel(string) save(string asis) replace(string)] varlist(namelist)

    // * Cross-option validation
    // Ensure excel is only present if using is present
    if `"`save'"' == "" & "`excel'" != "" {
        display as error "excel() option is only allowed when save() is also specified."
        exit 198
    }

    // replace only makes sense together with save
    if "`replace'" != "" & `"`save'"' == "" {
        display as error "option replace only allowed with save"
        exit 198
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

    // Check dependencies and set collapse command
    if "`fast'" != "" {
        capture which gtools
        if _rc == 111 {
            display as error "gtools is required for fast option. Install using " ///
                as smcl "{stata ssc install gtools}" as error " and then " ///
                as smcl "{stata gtools, upgrade}"
            exit 111
        }
        return local collapsecmd "gcollapse"
    }
    else {
        return local collapsecmd "collapse"
    }

    foreach var of local varlist {
        if "`var'" != "" {
            capture confirm numeric variable `var'
            if _rc {
                di as error "Variable `var' not numeric"
                exit 111
            }
        }
    }

end

// * Determines the data source
program define _argload, rclass
    syntax, [using(string) clear(string)]

    local _inmemory = c(filename) != "" | c(N) > 0 | c(k) > 0 | c(changed) == 1
    if `_inmemory' == 0 & "`using'" == "" {
        display as error "No data source for executing dtstat. Please specify a dataset using the 'using' or load the data into memory."
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