* dtfreq_test.do
* Comprehensive test suite for dtfreq.ado
* Date: June 1, 2025

version 16
clear frames
capture log close
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}
log using test/log/dtkit_test1.log, replace

// manually drop main program and subroutines
capture program drop dtfreq
capture program drop _xtab
capture program drop _xtab_core
capture program drop _binreshape
capture program drop _crosstotal
capture program drop _labelvars
capture program drop _toexcel
capture program drop _formatvars
capture program drop _argcheck
capture program drop _argload
capture mata: mata drop _xtab_core_calc()
run ado/dtfreq.ado

// manually drop main program and subroutines
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

// manually drop main program and subroutines
capture program drop dtstat
capture program drop _stats
capture program drop _collapsevars
capture program drop _byprocess
capture program drop _format
capture program drop _labelvars
capture program drop _formatvars
capture program drop _toexcel
capture program drop _argcheck
capture program drop _argload
run ado/dtstat.ado

* run ado/dtkit.ado

dtkit

dtkit, examples

dtkit, licenses

dtkit, test
