*! version 2.2.1 13nov2026
*! Program for managing the dtkit package installation

capture program drop dtkit
program dtkit
    version 16

    syntax, [          ///
        LICENSEs       ///
        Verbose        ///
        UPdate         ///
        UPgrade        ///
        examples       ///
        test           ///
        tests(str)     ///
        branch(str)    ///
        tag(str)       ///
        pluginstatus   ///
    ]

    // Consolidate update/upgrade
    local do_update = ("`update'" != "" | "`upgrade'" != "")

    if ( `"`branch'"' == "" ) local branch main
    if !inlist(`"`branch'"', "main", "develop") {
        display as error "{bf:Warning}: Branch `branch' is not intended for normal use."
    }

    local cwd `c(pwd)'
    local github https://raw.githubusercontent.com/bukanpeneliti/dtkit/`branch'

    if ( "`licenses'" == "licenses" ) {
        display `"dtkit is {browse "https://github.com/bukanpeneliti/dtkit/blob/main/LICENSE":MIT-licensed}"'
        display ""
        display "This package provides data analysis utilities for Stata."
        display ""

        if ( "`verbose'" != "" ) {
            dtkit_licenses
        }

        if ( `"`do_update'`examples'`test'`tests'"' == `""' ) {
            exit 0
        }
    }

    if (`do_update') {
        capture net uninstall dtkit
        net install dtkit, from(`github') replace
        dtkit_sync_dtparquet_plugin, tag(`tag')
        if _rc {
            exit _rc
        }
        if ( `"`examples'`test'`tests'"' == `""' ) {
            exit 0
        }
    }

    if ("`pluginstatus'" != "") {
        dtkit_plugin_status
        if ( `"`examples'`test'`tests'"' == `""' ) {
            exit 0
        }
    }

    if ( "`examples'" != "" ) {
        dtkit_showcase
        if ( "`test'`tests'" == "" ) {
            exit 0
        }
    }

    if ( `"`test'`tests'"' != "" ) {
        local t_basic dtfreq dtstat dtmeta dtparquet
        local t_known basic dtfreq dtstat dtmeta dtparquet
        local t_extra: list tests - t_known

        if ( `:list sizeof t_extra' ) {
            display `"(unknown tests detected: `t_extra'; will try to run anyway)"'
        }

        if ( `"`tests'"' == "" ) {
            display as text "{bf:Note:} Running basic unit tests for dtkit components."
        }
        else {
            display as text "{bf:Note:} Running unit tests: `tests'"
        }
        display as text "Are you sure you want to run them? (yes/no)", _request(DTKIT_TESTS)
        if inlist(`"${DTKIT_TESTS}"', "y", "yes") {
            global DTKIT_TESTS
            capture noisily dtkit_run_tests `tests'
            exit _rc
        }
        else {
            global DTKIT_TESTS
            exit 0
        }
    }

    display "dtkit: Data Toolkit for Stata"
    display "Available commands: dtfreq, dtstat, dtmeta, dtparquet"
    display ""
    display as smcl "Usage examples: {stata dtkit, examples}"
    display as smcl "Package update: {stata dtkit, update}"
    display as smcl "License info: {stata dtkit, licenses}"
    display ""
    display "Version info:"
    which dtkit
    capture which dtfreq
    if !_rc display "  dtfreq: available"
    else display "  dtfreq: not found"
    capture which dtstat  
    if !_rc display "  dtstat: available"
    else display "  dtstat: not found"
    capture which dtmeta
    if !_rc display "  dtmeta: available" 
    else display "  dtmeta: not found"
    capture which dtparquet
    if !_rc display "  dtparquet: available"
    else display "  dtparquet: not found"
end

capture program drop dtkit_plugin_status
program dtkit_plugin_status
    version 16

    capture quietly findfile dtparquet.ado
    if _rc {
        display as error "dtparquet.ado not found on adopath."
        exit 601
    }

    local ado_path `"`r(fn)'"'
    local target_dir : subinstr local ado_path "dtparquet.ado" "", all
    local target_dll `"`target_dir'dtparquet.dll"'

    display "dtparquet plugin status"
    display "  ado path: `ado_path'"
    display "  dll path: `target_dll'"

    capture confirm file `"`target_dll'"'
    if _rc {
        display as error "  dll: missing"
    }
    else {
        display as result "  dll: present"
    }

    capture program drop dtparquet_plugin
    capture noisily program dtparquet_plugin, plugin using(`"`target_dll'"')
    if _rc {
        display as error "  plugin load: failed"
        exit 601
    }

    capture plugin call dtparquet_plugin, "version"
    if _rc {
        display as error "  plugin version: unavailable"
        exit _rc
    }

    display as result "  plugin version: `dtparquet_plugin_version'"
end

capture program drop dtkit_licenses
program dtkit_licenses
    display _n(1) `"{hline 79}"'                                                                     ///
         _n(1) `"dtkit license"'                                                                     ///
         _n(1) `""'                                                                                  ///
         _n(1) `"MIT License"'                                                                       ///
         _n(1) `""'                                                                                  ///
         _n(1) `"Copyright (c) 2025 Hafiz Arfyanto"'                                                ///
         _n(1) `""'                                                                                  ///
         _n(1) `"Permission is hereby granted, free of charge, to any person obtaining a copy"'      ///
         _n(1) `"of this software and associated documentation files (the "Software"), to"'          ///
         _n(1) `"deal in the Software without restriction, including without limitation the"'        ///
         _n(1) `"rights to use, copy, modify, merge, publish, distribute, sublicense, and/or"'       ///
         _n(1) `"sell copies of the Software, and to permit persons to whom the Software is"'        ///
         _n(1) `"furnished to do so, subject to the following conditions:"'                          ///
         _n(1) `""'                                                                                  ///
         _n(1) `"The above copyright notice and this permission notice shall be included in all"'    ///
         _n(1) `"copies or substantial portions of the Software."'                                   ///
         _n(1) `""'                                                                                  ///
         _n(1) `"THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR"'        ///
         _n(1) `"IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,"'          ///
         _n(1) `"FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL"'           ///
         _n(1) `"THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER"'        ///
         _n(1) `"LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,"'     ///
         _n(1) `"OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE"'     ///
         _n(1) `"SOFTWARE."'                                                                         ///
         _n(1) `""'
end

capture program drop dtkit_showcase
program dtkit_showcase
    display _n(1) "Running dtkit examples..."
    display _n(1) "{hline 60}"
    display "dtfreq: Frequency analysis and tabulation"
    display "{hline 40}"
    
    local examples_dir "`c(sysdir_plus)'d/dtkit/examples"
    capture confirm file "`examples_dir'/dtfreq_examples.do"
    if _rc == 0 {
        display "Running dtfreq examples..."
        quietly do "`examples_dir'/dtfreq_examples.do"
    }
    else {
        display "dtfreq examples not found, trying relative path..."
        capture do "examples/dtfreq_examples.do"
        if _rc != 0 display as error "Could not run dtfreq examples"
    }
    
    display _n(1) "{hline 40}"
    display "dtstat: Descriptive statistics"
    display "{hline 40}"
    
    capture confirm file "`examples_dir'/dtstat_examples.do"
    if _rc == 0 {
        display "Running dtstat examples..."
        quietly do "`examples_dir'/dtstat_examples.do"
    }
    else {
        display "dtstat examples not found, trying relative path..."
        capture do "examples/dtstat_examples.do"
        if _rc != 0 display as error "Could not run dtstat examples"
    }
    
    display _n(1) "{hline 40}"
    display "dtmeta: Metadata analysis"
    display "{hline 40}"
    
    capture confirm file "`examples_dir'/dtmeta_examples.do"
    if _rc == 0 {
        display "Running dtmeta examples..."
        quietly do "`examples_dir'/dtmeta_examples.do"
    }
    else {
        display "dtmeta examples not found, trying relative path..."
        capture do "examples/dtmeta_examples.do"
        if _rc != 0 display as error "Could not run dtmeta examples"
    }

    display _n(1) "{hline 40}"
    display "dtparquet: Parquet file interoperability"
    display "{hline 40}"
    
    capture confirm file "`examples_dir'/dtparquet_examples.do"
    if _rc == 0 {
        display "Running dtparquet examples..."
        quietly do "`examples_dir'/dtparquet_examples.do"
    }
    else {
        display "dtparquet examples not found, trying relative path..."
        capture do "examples/dtparquet_examples.do"
        if _rc != 0 display as error "Could not run dtparquet examples"
    }
    
    display _n(1) "{hline 60}"
    display "Examples completed. See output above for results."
end

capture program drop dtkit_run_tests
program dtkit_run_tests
    syntax [anything]
    
    local test_components `anything'
    if "`test_components'" == "" local test_components "basic"
    
    display "Running dtkit tests: `test_components'"
    
    foreach component in `test_components' {
        if "`component'" == "basic" {
            display _n(1) "Running basic functionality tests..."
            dtkit_test_basic
        }
        else if inlist("`component'", "dtfreq", "dtstat", "dtmeta", "dtparquet") {
            display _n(1) "Running `component' tests..."
            local test_dir "test/`component'"
            capture confirm file "`test_dir'/`component'_test1.do"
            if _rc == 0 {
                quietly do "`test_dir'/`component'_test1.do"
                display "`component' test 1: completed"
            }
            capture confirm file "`test_dir'/`component'_test2.do"
            if _rc == 0 {
                quietly do "`test_dir'/`component'_test2.do"
                display "`component' test 2: completed"
            }
            capture confirm file "`test_dir'/`component'_test3.do"
            if _rc == 0 {
                quietly do "`test_dir'/`component'_test3.do"
                display "`component' test 3: completed"
            }
        }
        else {
            display as error "Unknown test component: `component'"
        }
    }
end

capture program drop dtkit_test_basic
program dtkit_test_basic
    display "Testing dtkit components availability..."
    
    local commands dtfreq dtstat dtmeta dtparquet
    foreach cmd in `commands' {
        capture which `cmd'
        if _rc == 0 {
            display "  `cmd': {bf:OK}"
        }
        else {
            display "  `cmd': {bf:MISSING}"
        }
    }
    
    display "Basic tests completed."
end 

capture program drop dtkit_sync_dtparquet_plugin
program dtkit_sync_dtparquet_plugin
    version 16
    syntax [, tag(str)]

    if lower("`c(os)'") != "windows" {
        exit 0
    }

    if ustrregexm(`"`tag'"', "[ \t&|<>^]") ///
        | strpos(`"`tag'"', char(34)) > 0 ///
        | strpos(`"`tag'"', char(39)) > 0 {
        display as error "Invalid characters in tag() option."
        exit 198
    }

    capture quietly findfile dtparquet.ado
    if _rc {
        display as error "Could not locate dtparquet.ado after installation."
        exit 601
    }

    local ado_path `"`r(fn)'"'
    local target_dir : subinstr local ado_path "dtparquet.ado" "", all
    local target_dll `"`target_dir'dtparquet.dll"'

    // Clean up unlocked .old* files left over from earlier locked replacements
    local old_files : dir `"`target_dir'"' files "dtparquet.dll.old*"
    foreach old_file of local old_files {
        capture erase `"`target_dir'`old_file'"'
    }

    local release_base "https://github.com/bukanpeneliti/dtkit/releases"
    local dll_url `"`release_base'/latest/download/dtparquet.dll"'
    local sha_url `"`release_base'/latest/download/dtparquet.sha256"'
    if `"`tag'"' != "" {
        local dll_url `"`release_base'/download/`tag'/dtparquet.dll"'
        local sha_url `"`release_base'/download/`tag'/dtparquet.sha256"'
    }

    // Run shell commands from target_dir with relative names so directory
    // spaces never break cmd /c quoting.
    local orig_pwd `"`c(pwd)'"'
    capture noisily dtkit_sync_dtparquet_body ///
        `"`target_dir'"' `"`target_dll'"' `"`dll_url'"' `"`sha_url'"'
    local sync_rc = _rc
    quietly cd `"`orig_pwd'"'
    if `sync_rc' {
        exit `sync_rc'
    }
end

capture program drop dtkit_sync_dtparquet_body
program dtkit_sync_dtparquet_body
    version 16
    args target_dir target_dll dll_url sha_url

    quietly cd `"`target_dir'"'

    // Download checksum file first and validate format
    local sha_tmp "dtparquet_sha.tmp"
    capture noisily dtkit_download_file `"`sha_url'"' `"`sha_tmp'"'
    if _rc {
        capture erase `"`sha_tmp'"'
        display as error ///
            "Could not download dtparquet.sha256 from GitHub Releases."
        dtkit_sync_error_help `"`sha_url'"' `"`target_dir'"'
        exit 601
    }

    local expected_hash ""
    tempname fh
    file open `fh' using `"`sha_tmp'"', read text
    file read `fh' line
    while r(eof) == 0 & `"`expected_hash'"' == "" {
        local trimmed = trim(`"`line'"')
        if `"`trimmed'"' != "" {
            gettoken first_tok : trimmed
            local expected_hash = lower(trim(`"`first_tok'"'))
        }
        file read `fh' line
    }
    file close `fh'
    capture erase `"`sha_tmp'"'

    if !ustrregexm(`"`expected_hash'"', "^[0-9a-f]{64}$") {
        display as error ///
            "Invalid SHA256 checksum in downloaded dtparquet.sha256."
        dtkit_sync_error_help `"`sha_url'"' `"`target_dir'"'
        exit 601
    }

    // Compare local DLL hash without loading the DLL into Stata
    local has_local = 0
    capture confirm file "dtparquet.dll"
    if _rc == 0 {
        local has_local = 1
        capture dtkit_file_sha256 "dtparquet.dll"
        if _rc == 0 {
            if `"`r(sha256)'"' == `"`expected_hash'"' {
                display as result ///
                    "dtparquet plugin is already up to date: `target_dll'"
                exit 0
            }
        }
    }

    // Download to staging file and verify checksum before replacing target
    local staging_name "dtparquet.dll.new"
    capture erase `"`staging_name'"'
    capture noisily dtkit_download_file `"`dll_url'"' `"`staging_name'"'
    if _rc {
        capture erase `"`staging_name'"'
        display as error ///
            "Could not install dtparquet plugin binary from GitHub Releases."
        dtkit_sync_error_help `"`dll_url'"' `"`target_dir'"'
        exit 601
    }

    capture noisily dtkit_file_sha256 `"`staging_name'"'
    if _rc {
        capture erase `"`staging_name'"'
        display as error ///
            "Could not verify SHA256 checksum of downloaded dtparquet.dll."
        exit 601
    }

    local staged_hash `"`r(sha256)'"'
    if `"`staged_hash'"' != `"`expected_hash'"' {
        capture erase `"`staging_name'"'
        display as error ///
            "SHA256 verification failed for downloaded dtparquet.dll."
        display as text "  Expected: `expected_hash'"
        display as text "  Actual:   `staged_hash'"
        exit 601
    }

    // Check whether dtparquet_plugin is already loaded without loading it
    local plugin_loaded = 0
    capture plugin call dtparquet_plugin, "version"
    if _rc == 0 {
        local plugin_loaded = 1
    }

    // Rename existing DLL in place (works on Windows even while loaded)
    local old_name ""
    local old_locked = 0
    if `has_local' {
        local ts_date = subinstr("`c(current_date)'", " ", "", .)
        local ts_time = subinstr("`c(current_time)'", ":", "", .)
        local suffix `"`ts_date'_`ts_time'"'
        local old_name `"dtparquet.dll.old.`suffix'"'
        local attempt = 0
        capture confirm file `"`old_name'"'
        while _rc == 0 {
            local attempt = `attempt' + 1
            local old_name `"dtparquet.dll.old.`suffix'_`attempt'"'
            capture confirm file `"`old_name'"'
        }

        capture erase "dtparquet_ok.tmp"
        local mv_cmd "move /y dtparquet.dll `old_name' > nul 2>&1"
        local mv_cmd `"`mv_cmd' && echo OK > dtparquet_ok.tmp"'
        quietly shell cmd /c "`mv_cmd'"
        capture confirm file "dtparquet_ok.tmp"
        local move_rc = _rc
        capture erase "dtparquet_ok.tmp"
        if `move_rc' {
            capture erase `"`staging_name'"'
            display as error ///
                "Could not rename existing dtparquet.dll before update."
            display as text ///
                "Check write permissions in `target_dir' and retry."
            exit 601
        }
    }

    capture erase "dtparquet_ok.tmp"
    local ins_cmd "move /y `staging_name' dtparquet.dll > nul 2>&1"
    local ins_cmd `"`ins_cmd' && echo OK > dtparquet_ok.tmp"'
    quietly shell cmd /c "`ins_cmd'"
    capture confirm file "dtparquet_ok.tmp"
    local install_rc = _rc
    capture erase "dtparquet_ok.tmp"
    if `install_rc' {
        if `has_local' & `"`old_name'"' != "" {
            quietly shell cmd /c "move /y `old_name' dtparquet.dll > nul 2>&1"
        }
        capture erase `"`staging_name'"'
        display as error ///
            "Could not move verified dtparquet.dll into `target_dir'."
        exit 601
    }

    if `has_local' & `"`old_name'"' != "" {
        capture erase `"`old_name'"'
        if _rc {
            local old_locked = 1
        }
    }

    if `plugin_loaded' | `old_locked' {
        display as result "Updated dtparquet plugin on disk: `target_dll'"
        display as text "{bf:Note:} dtparquet.dll was in use; " ///
            "restart Stata to load the new DLL in this session."
    }
    else {
        display as result "Installed dtparquet plugin: `target_dll'"
    }
end

capture program drop dtkit_download_file
program dtkit_download_file
    version 16
    args url dest_name

    capture erase `"`dest_name'"'
    capture erase "dtparquet_ok.tmp"
    local dl_cmd "curl.exe -fsSL -o `dest_name' `url' > nul 2>&1"
    local dl_cmd `"`dl_cmd' && echo OK > dtparquet_ok.tmp"'
    quietly shell cmd /c "`dl_cmd'"

    capture confirm file "dtparquet_ok.tmp"
    local curl_ok = (_rc == 0)
    capture erase "dtparquet_ok.tmp"

    if `curl_ok' {
        capture confirm file `"`dest_name'"'
        if _rc == 0 {
            exit 0
        }
    }

    capture erase `"`dest_name'"'
    capture copy `"`url'"' `"`dest_name'"', replace
    if _rc {
        capture erase `"`dest_name'"'
        exit _rc
    }
    capture confirm file `"`dest_name'"'
    if _rc {
        exit 601
    }
end

capture program drop dtkit_file_sha256
program dtkit_file_sha256, rclass
    version 16
    args filename

    confirm file `"`filename'"'

    capture erase "dtparquet_hash.tmp"
    capture erase "dtparquet_ok.tmp"
    local hash_cmd "certutil -hashfile `filename' SHA256"
    local hash_cmd `"`hash_cmd' > dtparquet_hash.tmp 2>&1"'
    local hash_cmd `"`hash_cmd' && echo OK > dtparquet_ok.tmp"'
    quietly shell cmd /c "`hash_cmd'"

    local computed ""
    capture confirm file "dtparquet_ok.tmp"
    if _rc == 0 {
        tempname fh
        file open `fh' using "dtparquet_hash.tmp", read text
        file read `fh' line
        while r(eof) == 0 & `"`computed'"' == "" {
            local candidate : subinstr local line " " "", all
            local candidate = lower(trim(`"`candidate'"'))
            if ustrregexm(`"`candidate'"', "^[0-9a-f]{64}$") {
                local computed `"`candidate'"'
            }
            file read `fh' line
        }
        file close `fh'
    }

    capture erase "dtparquet_hash.tmp"
    capture erase "dtparquet_ok.tmp"

    if `"`computed'"' == "" {
        display as error "Could not compute SHA256 hash for `filename'."
        exit 601
    }

    return local sha256 `"`computed'"'
end

capture program drop dtkit_sync_error_help
program dtkit_sync_error_help
    version 16
    args asset_url target_dir

    display as text ""
    display as text "Try the following:"
    display as text ///
        "  1) Check internet/proxy access to GitHub release assets."
    display as text "  2) Retry: {stata dtkit, update}"
    display as text ///
        "  3) Manual install: place dtparquet.dll in `target_dir'"
    display as text ""
    display as text "Attempted asset URL:"
    display as text "  `asset_url'"
    display as text "Release page:"
    display as text "  https://github.com/bukanpeneliti/dtkit/releases"
end
