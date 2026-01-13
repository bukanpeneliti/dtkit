* dtparquet_test_pyarrow.do
* Test for _check_python with Python Install Manager (no pyarrow)
* Date: Jan 12, 2026

version 16
clear frames
capture log close
cd d:/OneDrive/MyWork/00personal/stata/dtkit

log using ado/ancillary_files/test/log/dtparquet_test_pyarrow.log, replace

// Manually drop and load programs
capture program drop dtparquet
capture program drop dtparquet_save
capture program drop dtparquet_use
capture program drop _apply_dtmeta
capture program drop _check_python
copy ado/dtparquet.ado "c:/Users/hafiz/ado/plus/d/dtparquet.ado", replace
copy ado/dtparquet.py "c:/Users/hafiz/ado/plus/d/dtparquet.py", replace

// Display test header
display _newline(2) "=========================================="
display "Starting dtparquet pyarrow Check Test"
display "Timestamp: " c(current_date) " " c(current_time)
display "==========================================" _newline

// Test Case: _check_python - pyarrow not installed (Python from Python Install Manager)
display _newline "=== TEST CASE: _check_python - pyarrow Not Installed ==="
python query
if r(initialized) == 1 {
    display as error "Python already initialized. Please restart Stata to run this test."
    exit 198
}
set python_exec "c:\Users\hafiz\AppData\Local\Python\bin\python.exe"
clear
capture dtparquet_save "test_pyarrow_check.parquet", replace
if _rc == 198 {
    display as result "Test completed successfully (caught pyarrow not installed error)"
}
else {
    display as error "Test failed: expected error 198 (pyarrow not installed), got " _rc
}

// Cleanup
capture erase "test_pyarrow_check.parquet"
capture set python_exec ""

display _newline "=========================================="
display "Test completed"
display "=========================================="

log close
