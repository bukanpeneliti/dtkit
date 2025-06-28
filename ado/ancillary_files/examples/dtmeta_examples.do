// * examples:
// Basic metadata extraction from data in memory

        sysuse auto
        dtmeta

// Extract metadata from external file

        dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta"

// Show detailed report with frame access commands

        dtmeta, report

// Export to Excel with file replacement

        dtmeta, save("dataset_metadata.xlsx") replace

// Work with variable metadata

        dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", clear
        frame _dtvars: list varname type format vallab

// Analyze value label coverage

        dtmeta
        frame _dtvars: generate has_vallab = (vallab != "")
        frame _dtvars: tab has_vallab

// Examine variable notes

        notes make: test note
        dtmeta
        frame _dtnotes: list varname _note_text

// Review dataset information

        dtmeta
        frame _dtinfo: list, noobs

// Comprehensive workflow with external data and export

        dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta", save("mydata_metadata.xlsx") replace report clear

// Clear memory after loading external data

        dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta", clear
