*! version 1.1.0 28Jun2025 Hafiz Arfyanto, hafizarfyanto@gmail.com
*! Program for managing the dtkit package installation

capture program drop dtkit
program dtkit
    version 16

    syntax, [          ///
        LICENSEs       ///
        Verbose        ///
        Install_latest ///
        Upgrade        ///
        showcase       ///
        examples       ///
        test           ///
        TESTs(str)     ///
        branch(str)    ///
    ]

    if ( `"`branch'"' == "" ) local branch main
    if !inlist(`"`branch'"', "main", "develop") {
        display as error "{bf:Warning}: Branch `branch' is not intended for normal use."
    }

    local cwd `c(pwd)'
    local github https://raw.githubusercontent.com/hafizarfyanto/dtkit/`branch'

    if ( "`licenses'" == "licenses" ) {
        display `"dtkit is {browse "https://github.com/hafizarfyanto/dtkit/blob/main/LICENSE":MIT-licensed}"'
        display ""
        display "This package provides data analysis utilities for Stata."
        display ""

        if ( "`verbose'" != "" ) {
            dtkit_licenses
        }

        if ( `"`install_latest'`upgrade'`showcase'`examples'`test'`tests'"' == `""' ) {
            exit 0
        }
    }

    if ( ("`install_latest'" == "install_latest") | ("`upgrade'" == "upgrade") ) {
        capture net uninstall dtkit
        net install dtkit, from(`github') replace
        if ( `"`showcase'`examples'`test'`tests'"' == `""' ) {
            exit 0
        }
    }

    if ( "`showcase'`examples'" != "" ) {
        dtkit_showcase
        if ( "`test'`tests'" == "" ) {
            exit 0
        }
    }

    if ( `"`test'`tests'"' != "" ) {
        local t_basic dtfreq dtstat dtmeta
        local t_known basic dtfreq dtstat dtmeta
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
    display "Available commands: dtfreq, dtstat, dtmeta"
    display ""
    display as smcl "Usage examples: {stata dtkit, examples}"
    display as smcl "Package upgrade: {stata dtkit, upgrade}"
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
        else if inlist("`component'", "dtfreq", "dtstat", "dtmeta") {
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
        }
        else {
            display as error "Unknown test component: `component'"
        }
    }
end

capture program drop dtkit_test_basic
program dtkit_test_basic
    display "Testing dtkit components availability..."
    
    local commands dtfreq dtstat dtmeta
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