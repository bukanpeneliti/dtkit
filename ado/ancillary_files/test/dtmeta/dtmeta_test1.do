* dtmeta_test1.do
* Offline test suite for dtmeta.ado: local datasets, option validation,
* Excel export, and metadata content. No internet access required.
* Remote data sources are covered in dtmeta_test2.do.
* Refactored Oct 9, 2026 from the former 12-case suite.

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
log using ado/ancillary_files/test/log/dtmeta_test1.log, replace

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
// Scratch directory for the Excel export cases. The directory and the files
// in it are gitignored and erased again at the end of the run.
local xldir "ado/ancillary_files/test/dtmeta/excel tmp"
capture mkdir "`xldir'"

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
// into the local macro `sheets'. Absent entries come back as "." and are
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
di "dtmeta offline test suite"
di "Timestamp: " c(current_date) " " c(current_time)
di "==========================================" _n

// ----------------------------------------------------------- test 1
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': no data in memory ==="
_dtmeta_reset
local err 0
capture noisily dtmeta
if _rc != 198 {
    di as error "  expected r(198) with no data source, got " _rc
    local ++err
}
foreach fr in _dtvars _dtnotes _dtlabel _dtinfo _dtsource {
    capture confirm frame `fr'
    if _rc == 0 {
        di as error "  frame `fr' left behind after the error"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 2
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': in-memory auto, frames and returned results ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    // read r(source_frame) before anything else posts results
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    foreach fr in _dtvars _dtlabel _dtinfo {
        capture confirm frame `fr'
        if _rc != 0 {
            di as error "  frame `fr' missing"
            local ++err
        }
    }
    capture confirm frame _dtnotes
    if _rc == 0 {
        di as error "  frame _dtnotes should be dropped when there are no notes"
        local ++err
    }
    frame _dtvars: count
    if r(N) != 12 {
        di as error "  _dtvars has " r(N) " rows, expected 12"
        local ++err
    }
    frame _dtlabel: count
    if r(N) != 2 {
        di as error "  _dtlabel has " r(N) " rows, expected 2"
        local ++err
    }
    frame _dtinfo: count
    if r(N) != 1 {
        di as error "  _dtinfo has " r(N) " rows, expected 1"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 3
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': value label rows for auto ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    frame _dtlabel: local t3_vallab1 = vallab[1]
    frame _dtlabel: local t3_value1 = value[1]
    frame _dtlabel: local t3_label1 = label[1]
    frame _dtlabel: local t3_vallab2 = vallab[2]
    frame _dtlabel: local t3_label2 = label[2]
    if "`t3_vallab1'" != "origin" | "`t3_vallab2'" != "origin" {
        di as error "  vallab is [`t3_vallab1'] and [`t3_vallab2'], expected origin"
        local ++err
    }
    if `t3_value1' != 0 {
        di as error "  first origin value is `t3_value1', expected 0"
        local ++err
    }
    if "`t3_label1'" != "Domestic" | "`t3_label2'" != "Foreign" {
        di as error "  origin labels are [`t3_label1'] and [`t3_label2'], expected Domestic and Foreign"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 4
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': value labels on nlsw88 ==="
_dtmeta_reset
sysuse nlsw88, clear
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    frame _dtlabel: count
    if r(N) != 42 {
        di as error "  _dtlabel has " r(N) " rows, expected 42"
        local ++err
    }
    frame _dtlabel: local t4_vallab = vallab[1]
    frame _dtlabel: local t4_label = label[1]
    if "`t4_vallab'" != "ccitylbl" | "`t4_label'" != "Not central city" {
        di as error "  first row is [`t4_vallab'] / [`t4_label'], expected ccitylbl / Not central city"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 5
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': variable notes and dataset note ==="
_dtmeta_reset
clear
set obs 5
gen x = _n
gen y = _n * 2
// notes stores the text after the colon verbatim, double quotes included, so
// the note text below is unquoted to keep the comparisons readable.
notes x: first note on x
notes x: second note on x
notes y: only note on y
notes _dta: dataset level note
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    capture confirm frame _dtnotes
    if _rc != 0 {
        di as error "  frame _dtnotes missing"
        local ++err
    }
    else {
        frame _dtnotes: count
        if r(N) != 3 {
            di as error "  _dtnotes has " r(N) " rows, expected 3"
            local ++err
        }
        frame _dtnotes: local t5_v1 = varname[1]
        frame _dtnotes: local t5_i1 = _note_id[1]
        frame _dtnotes: local t5_x1 = _note_text[1]
        frame _dtnotes: local t5_v2 = varname[2]
        frame _dtnotes: local t5_i2 = _note_id[2]
        frame _dtnotes: local t5_x2 = _note_text[2]
        frame _dtnotes: local t5_v3 = varname[3]
        frame _dtnotes: local t5_i3 = _note_id[3]
        frame _dtnotes: local t5_x3 = _note_text[3]
        if "`t5_v1'" != "x" | `t5_i1' != 1 | "`t5_x1'" != "first note on x" {
            di as error "  row 1 is [`t5_v1'] `t5_i1' [`t5_x1'], expected x 1 first note on x"
            local ++err
        }
        if "`t5_v2'" != "x" | `t5_i2' != 2 | "`t5_x2'" != "second note on x" {
            di as error "  row 2 is [`t5_v2'] `t5_i2' [`t5_x2'], expected x 2 second note on x"
            local ++err
        }
        if "`t5_v3'" != "y" | `t5_i3' != 1 | "`t5_x3'" != "only note on y" {
            di as error "  row 3 is [`t5_v3'] `t5_i3' [`t5_x3'], expected y 1 only note on y"
            local ++err
        }
    }
    frame _dtinfo: local t5_note = dta_note[1]
    frame _dtinfo: local t5_obs = dta_obs[1]
    frame _dtinfo: local t5_vars = dta_vars[1]
    if "`t5_note'" != "dataset level note" {
        di as error "  dta_note is [`t5_note'], expected dataset level note"
        local ++err
    }
    if `t5_obs' != 5 | `t5_vars' != 2 {
        di as error "  dta_obs is `t5_obs' and dta_vars is `t5_vars', expected 5 and 2"
        local ++err
    }
    capture confirm frame _dtlabel
    if _rc == 0 {
        di as error "  frame _dtlabel should be dropped when there are no value labels"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 6
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': dataset with no notes and no value labels ==="
_dtmeta_reset
clear
set obs 100
gen x = _n
gen y = x^2
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    foreach fr in _dtnotes _dtlabel {
        capture confirm frame `fr'
        if _rc == 0 {
            di as error "  frame `fr' should be dropped when empty"
            local ++err
        }
    }
    frame _dtvars: count
    if r(N) != 2 {
        di as error "  _dtvars has " r(N) " rows, expected 2"
        local ++err
    }
    frame _dtinfo: count
    if r(N) != 1 {
        di as error "  _dtinfo has " r(N) " rows, expected 1"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 7
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a local file with no data in memory ==="
_dtmeta_reset
sysuse auto, clear
tempfile fixture
quietly save `fixture'
_dtmeta_reset
local err 0
capture noisily dtmeta using "`fixture'"
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    if c(N) != 74 | c(k) != 12 {
        di as error "  default frame holds " c(N) " x " c(k) ", expected 74 x 12"
        local ++err
    }
    frame _dtvars: count
    if r(N) != 12 {
        di as error "  _dtvars has " r(N) " rows, expected 12"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 8
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a local file with data in memory ==="
_dtmeta_reset
sysuse auto, clear
tempfile fixture
quietly save `fixture'
_dtmeta_reset
sysuse nlsw88, clear
local n_before = c(N)
local k_before = c(k)
local err 0
capture noisily dtmeta using "`fixture'"
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
        frame _dtsource: count
        if r(N) != 74 {
            di as error "  _dtsource has " r(N) " rows, expected 74"
            local ++err
        }
    }
    if c(N) != `n_before' | c(k) != `k_before' {
        di as error "  data in memory changed to " c(N) " x " c(k) ", expected `n_before' x `k_before'"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ----------------------------------------------------------- test 9
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': using a local file with clear ==="
_dtmeta_reset
sysuse auto, clear
tempfile fixture
quietly save `fixture'
_dtmeta_reset
sysuse nlsw88, clear
local err 0
capture noisily dtmeta using "`fixture'", clear
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    if c(N) != 74 | c(k) != 12 {
        di as error "  default frame holds " c(N) " x " c(k) ", expected 74 x 12 after clear"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 10
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': option validation ==="
_dtmeta_reset
local err 0
capture noisily dtmeta, clear
if _rc != 198 {
    di as error "  clear without using returned " _rc ", expected 198"
    local ++err
}
sysuse auto, clear
capture noisily dtmeta, replace
if _rc != 198 {
    di as error "  replace without save returned " _rc ", expected 198"
    local ++err
}
capture noisily dtmeta, excel(sheet("test"))
if _rc != 198 {
    di as error "  unknown option returned " _rc ", expected 198"
    local ++err
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 11
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export and sheet names ==="
_dtmeta_reset
sysuse auto, clear
local xfile "`xldir'/basic.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta, save("`xfile'") replace
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
            foreach want in "(auto) Dataset metadata" ///
                           "(auto) Value label metadata" ///
                           "(auto) Variable metadata" {
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

// ---------------------------------------------------------- test 12
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export without an extension ==="
_dtmeta_reset
sysuse auto, clear
capture erase "`xldir'/noext.xlsx"
local err 0
capture noisily dtmeta, save("`xldir'/noext") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
capture confirm file "`xldir'/noext.xlsx"
if _rc != 0 {
    di as error "  `xldir'/noext.xlsx not created"
    local ++err
}
capture erase "`xldir'/noext.xlsx"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 13
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export to a path with spaces ==="
_dtmeta_reset
sysuse auto, clear
local xfile "`xldir'/file with space.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta, save("`xfile'") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
capture confirm file "`xfile'"
if _rc != 0 {
    di as error "  `xfile' not created"
    local ++err
}
capture erase "`xfile'"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 14
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export to a missing directory ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta, save("`xldir'/no-such-dir/out.xlsx") replace
if _rc != 601 {
    di as error "  missing directory returned " _rc ", expected 601"
    local ++err
}
capture confirm file "`xldir'/no-such-dir/out.xlsx"
if _rc == 0 {
    di as error "  file created in a directory that does not exist"
    local ++err
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 15
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export path traversal ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta, save("`xldir'/../escape.xlsx") replace
if _rc != 198 {
    di as error "  path traversal returned " _rc ", expected 198"
    local ++err
}
capture confirm file "ado/ancillary_files/test/escape.xlsx"
if _rc == 0 {
    di as error "  escape.xlsx written outside the scratch directory"
    local ++err
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 16
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export without replace on an existing file ==="
_dtmeta_reset
sysuse auto, clear
local xfile "`xldir'/again.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta, save("`xfile'")
if _rc != 0 {
    di as error "  first export returned " _rc
    local ++err
}
capture noisily dtmeta, save("`xfile'")
if _rc != 0 {
    di as error "  second export without replace returned " _rc
    local ++err
}
capture confirm file "`xfile'"
if _rc != 0 {
    di as error "  `xfile' not created"
    local ++err
}
capture erase "`xfile'"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 17
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': value labels with embedded line breaks ==="
_dtmeta_reset
clear
set obs 1
gen byte y_lf = 1
gen byte y_cr = 1
// uselabel round-trips label text through a temp do-file, so LF/CR text broke
// its line parser ("in not found", r(111)). Build such labels in Mata and keep
// the text out of macros: LF at position 6 with length 11, CR at position 9
// with length 15.
mata: st_vlmodify("dtmeta_lf", 1, "line1" + char(10) + "line2")
label values y_lf dtmeta_lf
mata: st_vlmodify("dtmeta_cr", 1, "carriage" + char(13) + "return")
label values y_cr dtmeta_cr
local err 0
capture noisily dtmeta
if _rc != 0 {
    di as error "  dtmeta returned " _rc
    local ++err
}
else {
    local lfrow = 0
    local crrow = 0
    frame _dtlabel: local t17_N = _N
    forvalues r = 1/`t17_N' {
        frame _dtlabel: local t17_vl = vallab[`r']
        if "`t17_vl'" == "dtmeta_lf" local lfrow `r'
        if "`t17_vl'" == "dtmeta_cr" local crrow `r'
    }
    if `lfrow' == 0 | `crrow' == 0 {
        di as error "  label rows not found in _dtlabel"
        local ++err
    }
    else {
        frame _dtlabel: mata: st_numscalar("t17_lf_at", strpos(st_sdata(`lfrow', "label"), char(10)))
        frame _dtlabel: mata: st_numscalar("t17_lf_len", strlen(st_sdata(`lfrow', "label")))
        if t17_lf_at != 6 | t17_lf_len != 11 {
            di as error "  LF text not preserved (at=" t17_lf_at " len=" t17_lf_len ")"
            local ++err
        }
        frame _dtlabel: mata: st_numscalar("t17_cr_at", strpos(st_sdata(`crrow', "label"), char(13)))
        frame _dtlabel: mata: st_numscalar("t17_cr_len", strlen(st_sdata(`crrow', "label")))
        if t17_cr_at != 9 | t17_cr_len != 15 {
            di as error "  CR text not preserved (at=" t17_cr_at " len=" t17_cr_len ")"
            local ++err
        }
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 18
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': report option ==="
_dtmeta_reset
sysuse auto, clear
local err 0
capture noisily dtmeta, report
if _rc != 0 {
    di as error "  dtmeta, report returned " _rc
    local ++err
}
else {
    if "`r(source_frame)'" != "default" {
        di as error "  r(source_frame) is [`r(source_frame)'], expected default"
        local ++err
    }
    frame _dtvars: count
    if r(N) != 12 {
        di as error "  _dtvars has " r(N) " rows, expected 12"
        local ++err
    }
    capture confirm frame _dtlabel
    if _rc != 0 {
        di as error "  frame _dtlabel missing after report"
        local ++err
    }
}
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 19
// Excel limits sheet names to 31 characters. A source file with a long name
// must still export, and every sheet name must still start with the file name
// (cut to fit).
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export from a source file with a long name ==="
_dtmeta_reset
sysuse auto, clear
quietly save "`xldir'/a_very_long_dataset_name.dta", replace
_dtmeta_reset
local xfile "`xldir'/longname.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta using "`xldir'/a_very_long_dataset_name.dta", save("`xfile'") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc ", expected 0"
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
        local nsheets = ustrlen("`sheets'") - ustrlen(subinstr("`sheets'", "[", "", .))
        if `nsheets' != 3 {
            di as error "  workbook has `nsheets' sheets, expected 3"
            local ++err
        }
        forvalues i = 1/3 {
            local w = r(worksheet_`i')
            if ustrlen("`w'") > 31 {
                di as error "  sheet [`w'] is longer than 31 characters"
                local ++err
            }
            if substr("`w'", 1, 9) != "(a_very_l" {
                di as error "  sheet [`w'] does not start with the file name"
                local ++err
            }
        }
    }
}
capture erase "`xfile'"
capture erase "`xldir'/a_very_long_dataset_name.dta"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// ---------------------------------------------------------- test 20
// With data in memory, using loads the file into the helper frame _dtsource.
// That frame holds raw data, not metadata, so the workbook must not contain it.
_dtmeta_next
local t `r(n)'
di _n "=== TEST `t': Excel export with using and data in memory ==="
_dtmeta_reset
sysuse auto, clear
quietly save "`xldir'/auto.dta", replace
_dtmeta_reset
sysuse nlsw88, clear
local xfile "`xldir'/withmem.xlsx"
capture erase "`xfile'"
local err 0
capture noisily dtmeta using "`xldir'/auto.dta", save("`xfile'") replace
if _rc != 0 {
    di as error "  dtmeta returned " _rc ", expected 0"
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
        local nsheets = ustrlen("`sheets'") - ustrlen(subinstr("`sheets'", "[", "", .))
        if `nsheets' != 3 {
            di as error "  workbook has `nsheets' sheets, expected 3"
            local ++err
        }
        foreach want in "(auto) Dataset metadata" ///
                       "(auto) Value label metadata" ///
                       "(auto) Variable metadata" {
            if strpos("`sheets'", "[`want']") == 0 {
                di as error "  sheet [`want'] missing from `sheets'"
                local ++err
            }
        }
    }
}
capture erase "`xfile'"
capture erase "`xldir'/auto.dta"
if `err' == 0 _dtmeta_ok `t'
else _dtmeta_bad `t'

// -------------------------------------------------------------- summary
_dtmeta_reset
foreach f in basic.xlsx noext.xlsx again.xlsx longname.xlsx withmem.xlsx {
    capture erase "`xldir'/`f'"
}
capture erase "`xldir'/file with space.xlsx"

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

di _n as text "Remote data sources are covered by dtmeta_test2.do, which needs internet access."
di _n(2) "=========================================="
di "Offline test suite completed"
di "Timestamp: " c(current_date) " " c(current_time)
di "=========================================="

log close
