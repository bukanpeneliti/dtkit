* cleanup_test_logs.do
* Cleanup script for dtkit test artifacts
* Removes log files and temporary files created during testing
* Date: June 28, 2025

version 16
set more off

// Device detection and path setup
if c(hostname) == "NUXS" {
    cd d:/OneDrive/MyWork/00personal/stata/dtkit
}
else {
    cd c:/Users/hafiz/OneDrive/MyWork/00personal/stata/dtkit
}

di "=========================================="
di "DTKIT TEST CLEANUP UTILITY"
di "=========================================="
di "Timestamp: " c(current_date) " " c(current_time)
di "Working directory: " c(pwd)
di "==========================================" _n

local files_removed 0
local files_not_found 0

// List of log files in the dedicated test log directory
local test_logs ""
local test_logs `test_logs' "ado/ancillary_files/test/log/dtfreq_test1.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/dtfreq_test2.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/dtmeta_test1.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/dtmeta_test2.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/dtstat_test1.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/dtstat_test2.log"
local test_logs `test_logs' "ado/ancillary_files/test/log/run_all_tests.log"

// List of log files that might still be created in project root (from /e flag usage)
local root_logs ""
local root_logs `root_logs' "dtfreq_test1.log"
local root_logs `root_logs' "dtfreq_test2.log"
local root_logs `root_logs' "dtmeta_test1.log"
local root_logs `root_logs' "dtmeta_test2.log"
local root_logs `root_logs' "dtstat_test1.log"
local root_logs `root_logs' "dtstat_test2.log"
local root_logs `root_logs' "dtfreq_examples.log"
local root_logs `root_logs' "dtmeta_examples.log"
local root_logs `root_logs' "dtstat_examples.log"
local root_logs `root_logs' "run_all_tests.log"
local root_logs `root_logs' "cleanup_test_logs.log"

// List of log files in old individual test directories (legacy from before centralization)
local old_test_logs ""
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtfreq/dtfreq_test1.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtfreq/dtfreq_test2.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtmeta/dtmeta_test1.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtmeta/dtmeta_test2.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtstat/dtstat_test1.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtstat/dtstat_test2.log"
local old_test_logs `old_test_logs' "ado/ancillary_files/test/dtkit/dtkit_test1.log"

// List of temporary Excel files that might be created
local temp_excel ""
local temp_excel `temp_excel' "dataset_metadata.xlsx"
local temp_excel `temp_excel' "_df.xlsx"
local temp_excel `temp_excel' "df2.xlsx"

di "Cleaning up log files from test log directory..."
foreach file in `test_logs' {
    capture confirm file "`file'"
    if _rc == 0 {
        capture erase "`file'"
        if _rc == 0 {
            di "  [OK] Removed: `file'"
            local ++files_removed
        }
        else {
            di "  [FAIL] Failed to remove: `file' (error `=_rc')"
        }
    }
    else {
        local ++files_not_found
    }
}

di _n "Cleaning up any remaining log files from project root..."
foreach file in `root_logs' {
    capture confirm file "`file'"
    if _rc == 0 {
        capture erase "`file'"
        if _rc == 0 {
            di "  [OK] Removed: `file'"
            local ++files_removed
        }
        else {
            di "  [FAIL] Failed to remove: `file' (error `=_rc')"
        }
    }
    else {
        local ++files_not_found
    }
}

di _n "Cleaning up legacy log files from individual test directories..."
foreach file in `old_test_logs' {
    capture confirm file "`file'"
    if _rc == 0 {
        capture erase "`file'"
        if _rc == 0 {
            di "  [OK] Removed: `file'"
            local ++files_removed
        }
        else {
            di "  [FAIL] Failed to remove: `file' (error `=_rc')"
        }
    }
    else {
        local ++files_not_found
    }
}

di _n "Cleaning up temporary Excel files..."
foreach file in `temp_excel' {
    capture confirm file "`file'"
    if _rc == 0 {
        capture erase "`file'"
        if _rc == 0 {
            di "  [OK] Removed: `file'"
            local ++files_removed
        }
        else {
            di "  [FAIL] Failed to remove: `file' (error `=_rc')"
        }
    }
    else {
        local ++files_not_found
    }
}

// Clean up any test-created Excel files in test directories
di _n "Cleaning up test Excel files in test directories..."
local test_excel_patterns ""
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/xlsfile.xlsx"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/no-space.xlsx"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/*.xls"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/file with space.xlsx"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/no-extension"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/no-extension.xlsx"
local test_excel_patterns `test_excel_patterns' "ado/ancillary_files/test/*/*_output.xlsx"

foreach pattern in `test_excel_patterns' {
    local files: dir "." files "`pattern'"
    foreach file in `files' {
        capture erase "`file'"
        if _rc == 0 {
            di "  [OK] Removed: `file'"
            local ++files_removed
        }
        else {
            di "  [FAIL] Failed to remove: `file' (error `=_rc')"
        }
    }
}

// Summary
di _n "=========================================="
di "CLEANUP SUMMARY"
di "=========================================="
di "Files removed: `files_removed'"
di "Files not found (already clean): `files_not_found'"

if `files_removed' > 0 {
    di _n as result "[OK] Cleanup completed successfully!"
    di as result "Removed `files_removed' test artifact files."
}
else {
    di _n as text "No files needed cleanup - workspace is already clean."
}

di _n "=========================================="
di "Cleanup completed: " c(current_date) " " c(current_time)
di "==========================================" 