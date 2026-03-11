*! version 2.0.1 11mar2026
*! 
*! Credits & Attribution:
*! This package (dtparquet) is inspired by and incorporates concepts 
*! and portions of code from the 'pq' command by Jon Rothbaum 
*! (U.S. Census Bureau). Specifically, parts of the Stata ADO 
*! command syntax and Rust plugin architecture were adapted 
*! from 'pq' (https://github.com/jrothbaum/stata_parquet_io).
*! 
*! LICENSE: MIT

program dtparquet
    version 16
    dtparquet__verify_plugin_version
    if _rc {
        exit _rc
    }
    _cleanup_orphaned

    // Check for NOTIMER option (supports: notimer, notime, notim, noti, not)
    local has_notimer 0
    local cmd_line `"`0'"'
    local rest_cmd `"`0'"'

    gettoken sub rest : rest_cmd
    local len = length("`sub'")
    local new_rest ""

    if strpos(`"`rest'"', `"""') > 0 {
        local new_rest = trim(`"`rest'"')
    }
    else {
        local word_count : word count `rest'
        forvalues j = 1/`word_count' {
            local word : word `j' of `rest'
            local word_lower = lower("`word'")
            local word_len = length("`word_lower'")
            if "`word_lower'" == substr("notimer", 1, max(5, `word_len')) {
                local has_notimer 1
            }
            else {
                local new_rest `"`new_rest' `word'"'
            }
        }
        local new_rest = trim("`new_rest'")
    }

    if `len' == 0 {
        display as error "Subcommand required: save, use, export, or import"
        exit 198
    }

    local start_time = clock("$S_TIME", "hms")
    local rc = 0
    local dtparquet__file ""
    local dtparquet__sub ""

    if "`sub'" == substr("save", 1, max(2, `len')) {
        local dtparquet__sub "save"
        gettoken fname : new_rest, parse(" ,")
        local dtparquet__file "`fname'"
        dtparquet_save `new_rest'
        local rc = _rc
    }
    else if "`sub'" == substr("use", 1, max(1, `len')) {
        local dtparquet__sub "use"
        local scan_rest `"`new_rest'"'
        gettoken tok scan_rest : scan_rest, bind
        while `"`tok'"' != "" {
            if lower(`"`tok'"') == "using" {
                gettoken fname scan_rest : scan_rest, bind
                local dtparquet__file "`fname'"
                local tok ""
            }
            else {
                gettoken tok scan_rest : scan_rest, bind
            }
        }
        dtparquet_use `new_rest'
        local rc = _rc
    }
    else if "`sub'" == substr("export", 1, max(3, `len')) {
        local dtparquet__sub "export"
        gettoken fname : new_rest, parse(" ")
        local dtparquet__file "`fname'"
        dtparquet_export `new_rest'
        local rc = _rc
    }
    else if "`sub'" == substr("import", 1, max(3, `len')) {
        local dtparquet__sub "import"
        local scan_rest `"`new_rest'"'
        gettoken tok scan_rest : scan_rest, bind
        while `"`tok'"' != "" {
            if lower(`"`tok'"') == "using" {
                gettoken fname scan_rest : scan_rest, bind
                local dtparquet__file "`fname'"
                local tok ""
            }
            else {
                gettoken tok scan_rest : scan_rest, bind
            }
        }
        dtparquet_import `new_rest'
        local rc = _rc
    }
    else {
        display as error "Unknown subcommand '`sub''"
        
        // Inference logic
        local has_using = strpos(`"`rest''"', " using ") > 0 | substr(trim(`"`rest''"'), 1, 5) == "using"
        
        if `has_using' {
            display as error "Did you mean 'use', 'export', or 'import'?"
            display as error "Try, for example: "
            display as smcl `"{stata dtparquet use `0'}"'
        }
        else {
            display as error "Did you mean 'save'?"
            display as error "Try, for example: "
            display as smcl `"{stata dtparquet save `0'}"'
        }
        exit 198
    }

    // Display timing only on success and if notimer not specified
    if `rc' == 0 & `has_notimer' == 0 {
        local elapsed = clock("$S_TIME", "hms") - `start_time'
        local elapsed_sec = `elapsed' / 1000

        // Format time string - decimal only for < 60s
        if `elapsed_sec' < 60 {
            local time_str = string(`elapsed_sec', "%9.1f") + "s"
        }
        else if `elapsed_sec' < 3600 {
            local mins = int(`elapsed_sec' / 60)
            local secs = int(mod(`elapsed_sec', 60))
            local time_str = "`mins'm `secs's"
        }
        else {
            local hrs = int(`elapsed_sec' / 3600)
            local mins = int(mod(`elapsed_sec', 3600) / 60)
            local secs = int(mod(mod(`elapsed_sec', 3600), 60))
            local time_str = "`hrs'h `mins'm `secs's"
        }

        // Clean up filename display
        local display_file = subinstr("`dtparquet__file'", `""', "", .)
        local display_file = subinstr("`display_file'", "'", "", .)
        local display_file = trim("`display_file'")

        // Display message based on subcommand
        if "`dtparquet__sub'" == "save" {
            display as result "Saved `display_file' in `time_str'"
        }
        else if "`dtparquet__sub'" == "use" {
            display as result "Loaded `display_file' in `time_str'"
        }
        else if "`dtparquet__sub'" == "export" {
            display as result "Exported `display_file' in `time_str'"
        }
        else if "`dtparquet__sub'" == "import" {
            display as result "Imported `display_file' in `time_str'"
        }
        else {
            display as result "Completed in `time_str'"
        }
    }

    exit `rc'
end

cap program drop dtparquet_plugin
local __dtparquet_plugin_path "dtparquet.dll"
capture confirm file "ado/dtparquet.dll"
if _rc == 0 {
    local __dtparquet_plugin_path "ado/dtparquet.dll"
}
else {
    capture quietly findfile dtparquet.dll
    if _rc == 0 {
        local __dtparquet_plugin_path `"`r(fn)'"'
    }
}
program dtparquet_plugin, plugin using(`"`__dtparquet_plugin_path'"')

capture program drop dtparquet__verify_plugin_version
program dtparquet__verify_plugin_version
    local expected_api "2.0"

    capture confirm file "ado/dtparquet.dll"
    if _rc != 0 {
        capture quietly findfile dtparquet.dll
    }
    if _rc {
        display as error "dtparquet plugin binary not found: dtparquet.dll"
        display as error "Run {bf:dtkit, update} to install the matching plugin binary."
        display as text "After first {bf:net install dtkit}, run {bf:dtkit, update} once."
        display as text "If download is blocked, manually install from:"
        display as text "  https://github.com/bukanpeneliti/dtkit/releases"
        exit 601
    }

    capture plugin call dtparquet_plugin, "version"
    if _rc {
        display as error "Unable to query dtparquet plugin version."
        display as error "Run {bf:dtkit, update} to install the matching plugin binary."
        display as text "You can inspect current state with {bf:dtkit, pluginstatus}."
        exit _rc
    }

    local plugin_version `"`dtparquet_plugin_version'"'
    if `"`plugin_version'"' == "" {
        display as error "dtparquet plugin did not report a version."
        display as error "Run {bf:dtkit, update} to install the matching plugin binary."
        exit 601
    }

    local plugin_api ""
    gettoken pv_major pv_rest : plugin_version, parse(".")
    if `"`pv_rest'"' != "" {
        local pv_rest = substr(`"`pv_rest'"', 2, .)
        gettoken pv_minor pv_patch : pv_rest, parse(".")
        if `"`pv_major'"' != "" & `"`pv_minor'"' != "" {
            local plugin_api `"`pv_major'.`pv_minor'"'
        }
    }

    if `"`plugin_api'"' == "" {
        display as error "Unrecognized dtparquet plugin version format: `plugin_version'"
        display as error "Run {bf:dtkit, update} to install matching components."
        exit 459
    }

    if `"`plugin_api'"' != `"`expected_api'"' {
        display as error "dtparquet API mismatch: ado `expected_api' vs plugin `plugin_api' (`plugin_version')"
        display as error "Run {bf:dtkit, update} to install matching components."
        display as text "You can inspect current state with {bf:dtkit, pluginstatus}."
        exit 459
    }
end

capture program drop dtparquet_save
program dtparquet_save
    syntax anything(name=filename) [, REplace NOLabel CHunksize(integer 0) COMPress(string) PARTitionby(string)]
    local is_nolabel = ("`nolabel'" != "")
    local compression = lower(trim("`compress'"))
    if "`compression'" == "" local compression fast
    if !inlist("`compression'", "fast", "balanced", "archive") & ///
        !inlist("`compression'", "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd") {
        display as error "invalid compress() value: `compress'"
        exit 198
    }
    local file = subinstr(`"`filename'"', `"""', "", .)
    local file : subinstr local file "\" "/", all

    if lower(substr("`file'", -8, .)) == ".parquet" {
        local file = substr("`file'", 1, length("`file'") - 8)
    }
    if "`partitionby'" == "" {
        local file "`file'.parquet"
    }

    if "`replace'" == "" confirm new file `"`file'"'
    local is_replace = ("`replace'" != "")

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
        local typei_raw `typei'
        local formati: format `vari'
        local varlabi ""
        local vallabi ""
        if `is_nolabel' == 0 {
            local varlabi : variable label `vari'
            local vallabi : value label `vari'
        }
        local str_length 0

        if ((substr("`typei'", 1, 3) == "str") & (lower("`typei'") != "strl")) {
            local str_length = substr("`typei'", 4, .)
            local typei string
        }

        local name_`i' `vari'
        local dtype_`i' `typei'
        local format_`i' `formati'
        local str_length_`i' `str_length'

        local dtmeta_varname_`i' `vari'
        local dtmeta_vartype_`i' `typei_raw'
        local dtmeta_varfmt_`i' `formati'
        local dtmeta_varlab_`i' `varlabi'
        local dtmeta_vallab_`i' `vallabi'
    }

    local schema_fields_json ""
    forvalues j = 1/`var_count' {
        if (`j' > 1) {
            local schema_fields_json `"`schema_fields_json',"'
        }
        local schema_fields_json `"`schema_fields_json'{""n"":""`name_`j''"",""d"":""`dtype_`j''"",""f"":""`format_`j''"",""l"":`str_length_`j''}"'
    }
    local schema_protocol_version 2
    local schema_payload_json `"{""v"":`schema_protocol_version',""f"":[`schema_fields_json']}"'

    local dtmeta_var_count `var_count'
    local dtmeta_label_count 0
    local dtmeta_dta_label ""
    local dtmeta_dta_obs 0
    local dtmeta_dta_vars 0
    local dtmeta_dta_ts ""
    local dtmeta_dta_note_count 0
    local dtmeta_var_note_count 0

    if `is_nolabel' == 0 {
        capture frame _dtinfo: local dtmeta_dta_label = dta_label[1]
        capture frame _dtinfo: local dtmeta_dta_obs = dta_obs[1]
        capture frame _dtinfo: local dtmeta_dta_vars = dta_vars[1]
        capture frame _dtinfo: local dtmeta_dta_ts = string(dta_ts[1], "%tc")
        capture frame _dtlabel: count

        if _rc == 0 {
            local n_lbl = r(N)
            local dtmeta_label_count `n_lbl'
            forvalues j = 1/`n_lbl' {
                frame _dtlabel: mata: st_local("dtmeta_label_name_`j'", st_sdata(`j', "vallab"))
                frame _dtlabel: local dtmeta_label_value_`j' = value[`j']
                frame _dtlabel: mata: st_local("dtmeta_label_text_`j'", st_sdata(`j', "label"))
            }
        }

        capture frame _dtinfo: count
        if _rc == 0 {
            local n_dta_note = r(N)
            local dtmeta_dta_note_count 0
            forvalues j = 1/`n_dta_note' {
                frame _dtinfo: mata: st_local("dta_note_j", st_sdata(`j', "dta_note"))
                if `"`dta_note_j'"' != "" {
                    local dtmeta_dta_note_count = `dtmeta_dta_note_count' + 1
                    local dtmeta_dta_note_`dtmeta_dta_note_count' `"`dta_note_j'"'
                }
            }
        }

        capture frame _dtnotes: count
        if _rc == 0 {
            local n_var_note = r(N)
            local dtmeta_var_note_count `n_var_note'
            forvalues j = 1/`n_var_note' {
                frame _dtnotes: mata: st_local("vn_j", st_sdata(`j', "varname"))
                frame _dtnotes: mata: st_local("vt_j", st_sdata(`j', "_note_text"))
                local dtmeta_var_note_var_`j' `"`vn_j'"'
                local dtmeta_var_note_text_`j' `"`vt_j'"'
            }
        }
    }

    local inc_labels = (`is_nolabel' == 0)
    local inc_notes  = (`is_nolabel' == 0)

    if `is_nolabel' {
        plugin call dtparquet_plugin, "save" "`file'" "from_macro" "0" "0" "" "from_macros" "`partitionby'" "`compression'" "-1" "`inc_labels'" "`inc_notes'" "`is_replace'" "`chunksize'"
    }
    else {
        capture plugin call dtparquet_plugin, "save" "`file'" "from_macro" "0" "0" "" "`schema_payload_json'" "`partitionby'" "`compression'" "-1" "`inc_labels'" "`inc_notes'" "`is_replace'" "`chunksize'"
        if _rc != 0 {
            plugin call dtparquet_plugin, "save" "`file'" "from_macro" "0" "0" "" "from_macros" "`partitionby'" "`compression'" "-1" "`inc_labels'" "`inc_notes'" "`is_replace'" "`chunksize'"
        }
    }


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

    syntax [anything(everything)] [if] [in] [, Clear NOLabel CHunksize(string) ALLstring CATMODE(string)]
    
    local vlist ""
    local if_exp ""
    local in_exp ""
    local filename ""
    local offset 0
    
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

    if `"`if'"' != "" {
        local if_exp `"`if'"'
    }
    if `"`in'"' != "" {
        local in_exp `"`in'"'
    }

    local is_nolabel = ("`nolabel'" != "")
    local is_clear = ("`clear'" != "")
    local is_int64_as_string = ("`allstring'" != "")
    local catmode_norm = lower(trim("`catmode'"))
    if "`catmode_norm'" == "" {
        local catmode_norm "encode"
    }
    if !inlist("`catmode_norm'", "encode", "raw", "both") {
        display as error "catmode() must be one of: encode, raw, both"
        exit 198
    }

    local file = subinstr(trim(`"`filename'"'), `"""', "", .)
    local file : subinstr local file "\" "/", all

    if lower(substr("`file'", -8, .)) == ".parquet" {
        local file = substr("`file'", 1, length("`file'") - 8)
    }
    local file "`file'.parquet"

    confirm file `"`file'"'
    if `is_clear' == 0 & (c(N) > 0 | c(k) > 0) error 4
    if `is_clear' == 1 quietly clear

    plugin call dtparquet_plugin, "describe" "`file'" "1" "1" "" "" "0" "0"
    plugin call dtparquet_plugin, "load_meta" "`file'"

    local n_rows = real("`n_rows'")
    local n_columns = real("`n_columns'")
    local vars_in_file
    forvalues i = 1/`n_columns' {
        local vars_in_file `vars_in_file' `name_`i''
    }

    if "`dtmeta_loaded'" == "1" {
        local n_meta = real("`dtmeta_var_count'")
        forvalues m = 1/`n_meta' {
            local mname `dtmeta_varname_`m''
            local mtype `dtmeta_vartype_`m''
            local mfmt `dtmeta_varfmt_`m''
            local i_original : list posof "`mname'" in vars_in_file
            if (`i_original' > 0 & `"`mtype'"' != "") {
                if (substr("`mtype'", 1, 3) == "str" | lower("`mtype'") == "strl") {
                    local resolved_mtype `mtype'
                    if (lower("`mtype'") == "strl") {
                        local observed_len = real("`string_length_`i_original''")
                        if (`observed_len' <= 2045) {
                            local resolved_mtype string
                        }
                    }
                    local type_`i_original' `resolved_mtype'
                    if `"`mfmt'"' != "" local format_`i_original' `mfmt'
                }
            }
        }
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
    local read_fields_json ""
    foreach vari of varlist * {
        local i = `i' + 1
        local i_matched : list posof "`vari'" in matched_vars
        if (`i_matched' > 0) {
            local i_original : list posof "`vari'" in vars_in_file
            local read_type `load_type_`i_original''
            if (substr("`read_type'", 1, 3) == "str" & lower("`read_type'") != "strl") {
                local read_type string
            }
            local v_to_read_index_`i_matched' `i'
            local v_to_read_name_`i_matched' `vari'
            local v_to_read_type_`i_matched' `read_type'
            local v_to_read_p_type_`i_matched' `polars_type_`i_original''

            local read_index = `i' - 1
            if (`i_matched' > 1) {
                local read_fields_json `"`read_fields_json',"'
            }
            local read_fields_json `"`read_fields_json'{""i"":`read_index',""n"":""`vari'"",""d"":""`polars_type_`i_original''"",""s"":""`read_type'""}"'
        }
    }

    local cast_json ""
    local n_cast_string_vars : word count `cast_string_vars'
    if (`n_cast_string_vars' > 0) {
        local q = char(34)
        local cast_json "{`q'string`q':["
        local cast_i = 0
        foreach cvar of local cast_string_vars {
            local cast_i = `cast_i' + 1
            if (`cast_i' > 1) {
                local cast_json "`cast_json',"
            }
            local cast_json "`cast_json'`q'`cvar'`q'"
        }
        local cast_json "`cast_json']}"
    }

    local schema_protocol_version 2
    local mapping `"{""v"":`schema_protocol_version',""f"":[`read_fields_json']}"'
    local parallelize ""
    local vertical_relaxed 0
    local asterisk_to_variable ""
    local sort ""
    local sql_if "`if_exp'"
    local sql_if = trim(subinstr(`"`sql_if'"', "if", "", 1))
    local batch_size = cond("`chunksize'" == "", 50000, real("`chunksize'"))
    local order_by_type 0
    local order_descending 0
    local plugin_offset = max(0, `offset' - 1)

    capture plugin call dtparquet_plugin, "read" "`file'" "from_macro" "`row_to_read'" "`plugin_offset'" "`sql_if'" "`mapping'" "`parallelize'" "`vertical_relaxed'" "`asterisk_to_variable'" "`sort'" "`order_by_type'" "`order_descending'" "`n_obs_already'" "0" "`cast_json'" "`batch_size'"
    if _rc != 0 {
        plugin call dtparquet_plugin, "read" "`file'" "from_macro" "`row_to_read'" "`plugin_offset'" "`sql_if'" "from_macros" "`parallelize'" "`vertical_relaxed'" "`asterisk_to_variable'" "`sort'" "`order_by_type'" "`order_descending'" "`n_obs_already'" "0" "0" "`batch_size'"
    }

    local n_loaded_rows = real("`n_loaded_rows'")
    if missing(`n_loaded_rows') local n_loaded_rows = `row_to_read'
    if (`n_loaded_rows' < `row_to_read') {
        local keep_to = `n_obs_already' + `n_loaded_rows'
        quietly keep in 1/`keep_to'
    }

    if "`dtmeta_loaded'" == "1" {
        local nvars_meta = real("`dtmeta_var_count'")
        forvalues i = 1/`nvars_meta' {
            local vname `dtmeta_varname_`i''
            local vtype_meta `dtmeta_vartype_`i''
            if (lower("`vtype_meta'") == "strl") {
                capture confirm variable `vname'
                if _rc == 0 {
                    local loaded_type: type `vname'
                    if regexm("`loaded_type'", "^str([0-9]+)$") {
                        capture recast strL `vname'
                    }
                }
            }
        }
    }

    if `is_nolabel' == 0 {
        if "`dtmeta_loaded'" == "1" {
            local apply_labels = (`"`dtmeta_dta_label'"' != "")

            if (`apply_labels') {
                local nlab = real("`dtmeta_label_count'")
                if (`nlab' > 0) {
                    forvalues j = 1/`nlab' {
                        local lname `dtmeta_label_name_`j''
                        local lvalue `dtmeta_label_value_`j''
                        local ltext `dtmeta_label_text_`j''
                        if "`lname'" != "" {
                            capture noisily label define `lname' `lvalue' `"`ltext'"', modify
                        }
                    }
                }
            }

            local nvars_meta = real("`dtmeta_var_count'")
            forvalues i = 1/`nvars_meta' {
                local vname `dtmeta_varname_`i''
                capture confirm variable `vname'
                if _rc == 0 {
                    local vlab `dtmeta_varlab_`i''
                    local vfmt `dtmeta_varfmt_`i''
                    local vlbl `dtmeta_vallab_`i''
                    if `"`vlab'"' != "" {
                        mata: st_varlabel(st_local("vname"), st_local("vlab"))
                    }
                    if `"`vfmt'"' != "" format `vname' `vfmt'
                    if (`apply_labels') {
                        if `"`vlbl'"' != "" {
                            mata: st_varvaluelabel(st_local("vname"), st_local("vlbl"))
                        }
                    }
                }
            }

            if (`apply_labels') {
                if `"`dtmeta_dta_label'"' != "" label data `"`dtmeta_dta_label'"'
            }

            local ndta = real("`dtmeta_dta_note_count'")
            if (`ndta' > 0) {
                capture notes drop _dta
                forvalues j = 1/`ndta' {
                    local dnote : copy local dtmeta_dta_note_`j'
                    if `"`dnote'"' != "" {
                        mata: st_global("_dta[note`j']", st_local("dnote"))
                    }
                }
                mata: st_global("_dta[note0]", st_local("ndta"))
            }

            local nvarnote = real("`dtmeta_var_note_count'")
            if (`nvarnote' > 0) {
                local seen_vars ""
                forvalues j = 1/`nvarnote' {
                    local vn : copy local dtmeta_var_note_var_`j'
                    local vt : copy local dtmeta_var_note_text_`j'
                    capture confirm variable `vn'
                    if (_rc == 0 & `"`vt'"' != "") {
                        if !`: list vn in seen_vars' {
                            capture notes drop `vn'
                            local seen_vars `seen_vars' `vn'
                            local count_`vn' 0
                        }
                        local count_`vn' = `count_`vn'' + 1
                        local c_vn `count_`vn''
                        mata: st_global(st_local("vn") + "[note" + st_local("c_vn") + "]", st_local("vt"))
                        mata: st_global(st_local("vn") + "[note0]", st_local("c_vn"))
                    }
                }
            }

            capture error 0
        }
        else {
            _apply_dtmeta
            if `is_int64_as_string' == 0 {
                local foreign_cat_vars
                foreach vari in `matched_vars' {
                    local i_original : list posof "`vari'" in vars_in_file
                    if (`i_original' > 0) {
                        local p_type `polars_type_`i_original''
                        if inlist("`p_type'", "categorical", "enum") {
                            local foreign_cat_vars `foreign_cat_vars' `vari'
                        }
                    }
                }
                if `"`foreign_cat_vars'"' != "" {
                    _apply_foreign_cat_labels `foreign_cat_vars', mode(`catmode_norm')
                }
            }
        }
    }

    capture error 0
end

capture program drop _apply_foreign_cat_labels
program _apply_foreign_cat_labels
    version 16
    syntax varlist, mode(string)

    local i = 0
    foreach vari of local varlist {
        local i = `i' + 1
        if "`mode'" == "raw" {
            continue
        }

        tempvar encoded
        quietly encode `vari', gen(`encoded')
        local tmp_label : value label `encoded'
        local stable_label = "cat_`i'"
        capture label drop `stable_label'
        label copy `tmp_label' `stable_label', replace
        label values `encoded' `stable_label'

        if "`mode'" == "both" {
            local id_name `vari'_id
            capture confirm variable `id_name'
            if _rc == 0 {
                local id_name `vari'_catid
                capture confirm variable `id_name'
                if _rc == 0 {
                    display as error "Cannot create categorical id variable for `vari' (name collision)"
                    exit 198
                }
            }
            gen long `id_name' = `encoded'
            label values `id_name' `stable_label'
            drop `encoded'
        }
        else {
            drop `vari'
            gen long `vari' = `encoded'
            label values `vari' `stable_label'
            drop `encoded'
        }
    }

    capture error 0
end

capture program drop dtparquet_export
program dtparquet_export
    local rest `"`0'"'
    gettoken pqfile rest : rest, bind
    gettoken using_kw rest : rest, bind
    gettoken dtafile rest : rest, bind
    if substr(`"`dtafile'"', -1, 1) == "," {
        local dtafile = substr(`"`dtafile'"', 1, length(`"`dtafile'"') - 1)
        local rest `", `rest'"'
    }
    local 0 `"`rest'"'
    syntax [, REplace NOLabel CHunksize(integer 50000)]
    local is_nolabel = ("`nolabel'" != "")

    if `"`pqfile'"' == "" | `"`dtafile'"' == "" | `"`using_kw'"' != "using" {
        display as error "Syntax: dtparquet export parquet_file using dta_file"
        exit 198
    }
    
    local target `"`pqfile'"'
    local target : subinstr local target `"""' "", all
    local target : subinstr local target "\" "/", all
    local source `"`dtafile'"'
    local source : subinstr local source `"""' "", all
    local source : subinstr local source "\" "/", all

    if lower(substr("`target'", -8, .)) == ".parquet" {
        local target = substr("`target'", 1, length("`target'") - 8)
    }
    local target "`target'.parquet"
    
    confirm file `"`source'"'
    if "`replace'" == "" confirm new file `"`target'"'

    tempname export_frame
    local orig_frame = c(frame)
    frame create `export_frame'
    frame change `export_frame'
    quietly use `"`source'"', clear

    local save_opts
    if "`replace'" != "" local save_opts `save_opts' replace
    if `is_nolabel' local save_opts `save_opts' nolabel

    if `"`save_opts'"' == "" {
        dtparquet_save "`target'"
    }
    else {
        dtparquet_save "`target'", `save_opts'
    }

    frame change `orig_frame'
    frame drop `export_frame'
end

capture program drop dtparquet_import
program dtparquet_import
    local rest `"`0'"'
    gettoken dtafile rest : rest, bind
    gettoken using_kw rest : rest, bind
    gettoken pqfile rest : rest, bind
    if substr(`"`pqfile'"', -1, 1) == "," {
        local pqfile = substr(`"`pqfile'"', 1, length(`"`pqfile'"') - 1)
        local rest `", `rest'"'
    }
    local 0 `"`rest'"'
    syntax [, REplace NOLabel CHunksize(integer 50000) ALLstring]

    if `"`dtafile'"' == "" | `"`pqfile'"' == "" | `"`using_kw'"' != "using" {
        display as error "Syntax: dtparquet import dta_file using parquet_file"
        exit 198
    }

    local target `"`dtafile'"'
    local target : subinstr local target `"""' "", all
    local target : subinstr local target "\" "/", all
    local source `"`pqfile'"'
    local source : subinstr local source `"""' "", all
    local source : subinstr local source "\" "/", all

    if lower(substr("`source'", -8, .)) == ".parquet" {
        local source = substr("`source'", 1, length("`source'") - 8)
    }
    local source "`source'.parquet"

    confirm file `"`source'"'
    if "`replace'" == "" confirm new file `"`target'"'

    tempname import_frame
    local orig_frame = c(frame)
    frame create `import_frame'
    frame change `import_frame'

    local import_read_started = clock("$S_TIME", "hms")

    if "`nolabel'" == "" & "`allstring'" == "" {
        dtparquet_use using `"`source'"', clear
    }
    else if "`nolabel'" != "" & "`allstring'" == "" {
        dtparquet_use using `"`source'"', clear nolabel
    }
    else if "`nolabel'" == "" & "`allstring'" != "" {
        dtparquet_use using `"`source'"', clear allstring
    }
    else {
        dtparquet_use using `"`source'"', clear nolabel allstring
    }

    local import_read_elapsed_ms = clock("$S_TIME", "hms") - `import_read_started'
    if (`import_read_elapsed_ms' < 0) local import_read_elapsed_ms = 0

    local import_save_started = clock("$S_TIME", "hms")

    if "`replace'" == "" {
        quietly save `"`target'"'
    }
    else {
        quietly save `"`target'"', `replace'
    }

    local import_save_elapsed_ms = clock("$S_TIME", "hms") - `import_save_started'
    if (`import_save_elapsed_ms' < 0) local import_save_elapsed_ms = 0
    global dtparquet_import_read_elapsed_ms `import_read_elapsed_ms'
    global dtparquet_import_save_elapsed_ms `import_save_elapsed_ms'

    frame change `orig_frame'
    frame drop `import_frame'
end

capture program drop _apply_dtmeta
program _apply_dtmeta
    local use_macro_meta 0
    if "`dtmeta_var_count'" != "" {
        local use_macro_meta = (real("`dtmeta_var_count'") > 0)
    }

    if `use_macro_meta' {
        local nlab = real("`dtmeta_label_count'")
        if (`nlab' > 0) {
            forvalues j = 1/`nlab' {
                local lname `dtmeta_label_name_`j''
                local lvalue `dtmeta_label_value_`j''
                local ltext `dtmeta_label_text_`j''
                if "`lname'" != "" {
                    capture noisily label define `lname' `lvalue' `"`ltext'"', modify
                }
            }
        }

        local nvars = real("`dtmeta_var_count'")
        forvalues i = 1/`nvars' {
            local vname `dtmeta_varname_`i''
            capture confirm variable `vname'
            if _rc == 0 {
                local vlab `dtmeta_varlab_`i''
                local vfmt `dtmeta_varfmt_`i''
                local vlbl `dtmeta_vallab_`i''
                if `"`vlab'"' != "" label variable `vname' `"`vlab'"'
                if `"`vfmt'"' != "" format `vname' `vfmt'
                if `"`vlbl'"' != "" capture label values `vname' `vlbl'
            }
        }

        if `"`dtmeta_dta_label'"' != "" label data `"`dtmeta_dta_label'"'
        exit 0
    }

    // Restore variable labels and formats from _dtvars
    capture frame _dtvars: count
    if _rc == 0 {
        local nvars = r(N)
        if `nvars' > 0 {
            forvalues i = 1/`nvars' {
                frame _dtvars: mata: st_local("vname", st_sdata(`i', "varname"))
                capture confirm variable `vname'
                if _rc == 0 {
                    frame _dtvars: mata: st_local("vlab", st_sdata(`i', "varlab"))
                    frame _dtvars: mata: st_local("vfmt", st_sdata(`i', "format"))
                    frame _dtvars: mata: st_local("vlbl", st_sdata(`i', "vallab"))
                    if `"`vlab'"' != "" {
                        mata: st_varlabel(st_local("vname"), st_local("vlab"))
                    }
                    if `"`vfmt'"' != "" format `vname' `vfmt'
                    if `"`vlbl'"' != "" {
                        mata: st_varvaluelabel(st_local("vname"), st_local("vlbl"))
                    }
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
                        frame `curlblfr': mata: st_local("txt", st_sdata(`j', "label"))
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
            local seen_vars ""
            forvalues i = 1/`nnotes' {
                frame _dtnotes: mata: st_local("vname", st_sdata(`i', "varname"))
                capture confirm variable `vname'
                if _rc == 0 {
                    if !`: list vname in seen_vars' {
                        capture notes drop `vname'
                        local seen_vars `seen_vars' `vname'
                        local count_`vname' 0
                    }
                    local count_`vname' = `count_`vname'' + 1
                    local c_vn `count_`vname''
                    frame _dtnotes: mata: st_local("vnote", st_sdata(`i', "_note_text"))
                    mata: st_global(st_local("vname") + "[note" + st_local("c_vn") + "]", st_local("vnote"))
                    mata: st_global(st_local("vname") + "[note0]", st_local("c_vn"))
                }
            }
        }
    }
    
    // Restore dataset info from _dtinfo
    capture frame _dtinfo: count
    if _rc == 0 {
        local ninfo = r(N)
        if `ninfo' > 0 {
            frame _dtinfo: mata: st_local("dlab", st_sdata(1, "dta_label"))
            if `"`dlab'"' != "" label data `"`dlab'"'
            
            local dta_note_count 0
            forvalues i = 1/`ninfo' {
                frame _dtinfo: mata: st_local("dnote", st_sdata(`i', "dta_note"))
                if `"`dnote'"' != "" {
                    if `dta_note_count' == 0 capture notes drop _dta
                    local dta_note_count = `dta_note_count' + 1
                    local c_dn `dta_note_count'
                    mata: st_global("_dta[note" + st_local("c_dn") + "]", st_local("dnote"))
                }
            }
            if `dta_note_count' > 0 {
                local c_dn `dta_note_count'
                mata: st_global("_dta[note0]", st_local("c_dn"))
            }
        }
    }
    
    // Cleanup
    foreach fr in _dtvars _dtlabel _dtnotes _dtinfo {
        capture frame drop `fr'
    }

    capture error 0
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
    else if ("`type_new'" == "date")     local type_new float
    else if regexm("`type_new'", "^str([0-9]+)$") {
        local string_length = max(1, real(regexs(1)))
        local type_new string
    }
    else if ("`type_new'" == "string")   local type_str str`string_length'

    if ("`type_new'" == "string") local type_str str`string_length'

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
    else if (lower("`type_new'") == "strl") {
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
    exit 0
end

capture program drop _cleanup_orphaned
program _cleanup_orphaned
    version 16
    quietly frame dir
    foreach frame in `r(frames)' {
        if strpos("`frame'", "_dtparquet_") == 1 capture frame drop `frame'
        if inlist("`frame'", "_dtvars", "_dtlabel", "_dtnotes", "_dtinfo") capture frame drop `frame'
    }
end
