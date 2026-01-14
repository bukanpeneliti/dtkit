*! dtparquet_examples.do - Examples for dtparquet command
*! Version 1.0.0 14jan2026

* Setup: Create a dummy dataset
sysuse auto, clear

* 1. Save the current dataset to Parquet
dtparquet save "myauto.parquet", replace

* 2. Load specific variables from a Parquet file
dtparquet use price mpg using "myauto.parquet", clear
describe

* 3. Load using 'use' subcommand (alternative syntax)
dtparquet use "myauto.parquet", clear
describe

* 4. Export a large .dta file to Parquet without loading it into memory
* First save as .dta
save "temp_auto.dta", replace
dtparquet export "results.parquet" using "temp_auto.dta", replace

* 5. Convert a Parquet file to Stata format on disk
dtparquet import "final.dta" using "results.parquet", replace

* Cleanup
capture erase "myauto.parquet"
capture erase "temp_auto.dta"
capture erase "results.parquet"
capture erase "final.dta"

exit
