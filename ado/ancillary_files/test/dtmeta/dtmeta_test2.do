* dtmeta_test2.do
* Network test suite for dtmeta.ado: remote data sources read through using.
* Every case needs internet access. The offline suite is dtmeta_test1.do.
* Refactored Oct 9, 2026 from the former 11-case suite: the remote file is
* fetched once into a local fixture and reused, so each remaining download
* covers a distinct code path.

version 16
clear frames
capture log close
capture confirm file "ado/dtmeta.ado"
if _rc != 0 {
    if c(hostname) == "NUXS" {
        cd d:/OneDrive/MyWork/00personal/stata/dtkit
    }
    else {
        cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
    }
}
log using ado/ancillary_files/test/log/dtmeta_test2.log, replace

// drop the command and every subroutine so the run tests this source file
capture program drop dtmeta
capture program drop _makevars
capture program drop _makevarnotes
capture program drop _makevallab
capture program drop _makedtainfo
capture program drop _isempty
capture program drop _labelframes
capture program drop _toexcel
capture program drop _argload
capture program drop _makereport
run ado/dtmeta.ado

// ------------------------------------------------------------------ setup
// Scratch directory for the exported workbooks and the downloaded fixture.
local xldir "ado/ancillary_files/test/dtmeta/excel tmp"
capture mkdir "`xldir'"

// fullauto.dta holds 74 observations and 18 variables. The fixture keeps a
// short name because dtmeta builds Excel sheet names from the source
// filename, and Excel rejects sheet names longer than 31 characters.
local url_auto "https://www.stata-press.com/data/r18/fullauto.dta"
local url_nlsw "https://www.stata-press.com/data/r18/nlsw88.dta"
local fixture "`xldir'/fa.dta"

// ----------------------------------------------------------------- harness
// dtmeta leaves frames behind, and a test that checks for a missing frame
// depends on earlier tests not having created it. Every test therefore starts
// from the same state: no dtmeta frames and no data in memory.
capture program drop _dtmeta_reset
program define _dtmeta_reset
    capture frame change default
    capture frames dir _dt*
    capture frame drop `r(frames)'
    clear
end

global dtmeta_total 0
global dtmeta_pass ""
global dtmeta_fail ""

capture program drop _dtmeta_next _dtmeta_ok _dtmeta_bad
program define _dtmeta_next, rclass
    global dtmeta_total = $dtmeta_total + 1
    return local n "$dtmeta_total"
end

program define _dtmeta_ok
    args n
    global dtmeta_pass "${dtmeta_pass} `n'"
    di as result "  PASS: test `n'"
end

program define _dtmeta_bad
    args n
    global dtmeta_fail "${dtmeta_fail} `n'"
    di as error "  FAIL: test `n'"
end

// _dtmeta_sheets: collect the sheet names posted by import excel ..., describe
// into the local macro `sheets`. Absent entries come back as "." and are
// skipped.
capture program drop _dtmeta_sheets
program define _dtmeta_sheets
    local sheets ""
    forvalues i = 1/8 {
        local w ""
        capture local w = r(worksheet_`i')
        if "`w'" != "" & "`w'" != "." local sheets "`sheets'[`w']"
    }
    c_local sheets "`sheets'"
end

di _n(2) "=========================================="
di "dtmeta network test suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// ----------------------------------------------------------- test 1
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': fetch the remote fixture once ==="
_dtmeta_reset
capture erase "`fixture'"
local err 0
capture noisily use "`url_auto'", clear
if _rc != 0 {
    di as error "  could not read `url_auto', rc " _rc
    local ++err
}
else {
    if c(N) != 74 | c(k) != 18 {
        di as error "  remote file holds " c(N) " x " c(k) ", expected 74 x 18"
        local ++err
    }
    capture noisily save "`fixture'", replace
    if _rc != 0 {
        di as error "  could not save the fixture, rc " _rc
        local ++err
    }
    capture confirm file "`fixture'"
    if _rc != 0 {
        di as error "  `fixture' not created"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 2
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a remote file with no data in memory ==="
_dtmeta_reset
local err 0
capture noisily dtmeta using "`url_auto'"
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    if c(N) != 74 | c(k) != 18 {
        di as error "  default frame holds " c(N) " x " c(k) ", expected 74 x 18"
        local ++err
    }
    frame _dtvars: count
    if r(N) != 18 {
        di as error "  _dtvars has " r(N) " rows, expected 18"
        local ++err
    }
    frame _dtinfo: local t2_obs = dta_obs[1]
    if `t2_obs' != 74 {
        di as error "  dta_obs is `t2_obs', expected 74"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 3
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a remote file with data in memory ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta using "`url_auto'"
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "_dtsource" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected _dtsource"
        local ++err
    }
    capture confirm frame _dtsource
    if _rc != 0 {
        di as error "  frame _dtsource missing"
        local ++err
    }
    else {
        frame _dtsource: local t3_k = c(k)
        frame _dtsource: local t3_N = c(N)
        if `t3_k' != 18 | `t3_N' != 74 {
            di as error "  _dtsource holds " `t3_N' " x " `t3_k' ", expected 74 x 18"
            local ++err
        }
    }
    if c(N) != 74 | c(k) != 12 {
        di as error "  data in memory changed to " c(N) " x " c(k) ", expected 74 x 12"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 4
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a remote file with clear ==="
_dtmeta_reset
sysuse nlsw88, clear
local err 0
capture noisily dtmeta using "`url_auto'", clear
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    if c(N) != 74 | c(k) != 18 {
        di as error "  default frame holds " c(N) " x " c(k) ", expected 74 x 18 after clear"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 5
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export from a remote source ==="
_dtmeta_reset
local xfile "`xldir'/remote.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta using "`url_auto'", save("`xfile'") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    capture confirm file "`xfile'"
    if _rc != 0 {
        di as error "  `xfile' not created"
        local ++err
    }
    else {
        capture noisily import excel using "`xfile'", describe
        if _rc != 0 {
            di as error "  workbook not readable, rc " _rc
            local ++err
        }
        else {
            _dtmeta_sheets
            di as text "  sheets: `sheets'"
            foreach want in "(fullauto) Dataset metadata" ///
                           "(fullauto) Value label metadata" ///
                           "(fullauto) Variable metadata" {
                if strpos("`sheets'", "[`want']") == 0 {
                    di as error "  sheet [`want'] missing from `sheets'"
                    local ++err
                }
            }
        }
    }
}
capture erase "`xfile'"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 6
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export from the local fixture ==="
_dtmeta_reset
sysuse auto, clear
local xfile "`xldir'/fixture.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta using "`fixture'", save("`xfile'") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    capture confirm file "`xfile'"
    if _rc != 0 {
        di as error "  `xfile' not created"
        local ++err
    }
    else {
        capture noisily import excel using "`xfile'", describe
        if _rc != 0 {
            di as error "  workbook not readable, rc " _rc
            local ++err
        }
        else {
            _dtmeta_sheets
            di as text "  sheets: `sheets'"
            foreach want in "(fa) Dataset metadata" ///
                           "(fa) Value label metadata" ///
                           "(fa) Variable metadata" {
                if strpos("`sheets'", "[`want']") == 0 {
                    di as error "  sheet [`want'] missing from `sheets'"
                    local ++err
                }
            }
        }
    }
}
capture erase "`xfile'"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 7
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using remote nlsw88 with clear ==="
_dtmeta_reset
local err 0
capture noisily dtmeta using "`url_nlsw'", clear
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    frame _dtvars: count
    if r(N) != c(k) {
        di as error "  _dtvars has " r(N) " rows, expected " c(k)
        local ++err
    }
    frame _dtlabel: count
    if r(N) == 0 {
        di as error "  _dtlabel is empty, nlsw88 carries value labels"
        local ++err
    }
    frame _dtinfo: local t7_obs = dta_obs[1]
    if `t7_obs' != c(N) {
        di as error "  dta_obs is `t7_obs', expected " c(N)
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 8
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': webuse nlswork, dataset note and size ==="
_dtmeta_reset
local err 0
capture noisily webuse nlswork, clear
if _rc != 0 {
    di as error "  webuse returned " _rc
    local ++err
}
else {
    capture noisily dtmeta
    if _rc != 0 {
        di as error "  dtmeta returned " _rc
        local ++err
    }
    else {
        frame _dtvars: count
        if r(N) != 21 {
            di as error "  _dtvars has " r(N) " rows, expected 21"
            local ++err
        }
        frame _dtinfo: local t8_note = dta_note[1]
        frame _dtinfo: local t8_obs = dta_obs[1]
        if `=strlen("`t8_note'")' < 20 {
            di as error "  dta_note is [`t8_note'], expected the NLSY citation"
            local ++err
        }
        if `t8_obs' != 28534 {
            di as error "  dta_obs is `t8_obs', expected 28534"
            local ++err
        }
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// -------------------------------------------------------------- summary
_dtmeta_reset
capture erase "`fixture'"
capture erase "`xldir'/remote.xlsx"
capture erase "`xldir'/fixture.xlsx"

di _n(2) "=========================================="
di "TEST SUMMARY"
di "=========================================="
local num_passed: word count $dtmeta_pass
local num_failed: word count $dtmeta_fail
di as text "Total tests run: " as result $dtmeta_total
di as text "Tests passed: " as result `num_passed' as text " (" as result %4.1f (`num_passed'/$dtmeta_total*100) as text "%)"
di as text "Tests failed: " as result `num_failed' as text " (" as result %4.1f (`num_failed'/$dtmeta_total*100) as text "%)"

if `num_passed' > 0 {
    di _n as text "PASSED TESTS:"
    foreach test in $dtmeta_pass {
        di as result "  PASSED: Test `test'"
    }
}

if `num_failed' > 0 {
    di _n as text "FAILED TESTS:"
    foreach test in $dtmeta_fail {
        di as error "  FAILED: Test `test'"
    }
}

di _n as text "Overall Status: " _continue
if `num_failed' == 0 {
    di as result "ALL TESTS PASSED!"
}
else {
    di as error "`num_failed' TEST(S) FAILED"
}

di _n as text "Run dtmeta_test1.do for the offline suite."
di _n(2) "=========================================="
di "Network test suite completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "=========================================="

log close
