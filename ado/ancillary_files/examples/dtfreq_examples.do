// Objective: Show frequency tables, binary output, and numeric formats.
version 16

global fnum "01"
global fmain "dtfreq"
global ftype "examples"
global vernum "v01"
global fname "${fnum}_${fmain}_${ftype}_${vernum}"
global logname "${ftype}_${vernum}"

// * source data
capture frame create nlsw88
frame nlsw88: sysuse nlsw88.dta, clear

// * one-way table
frame nlsw88: dtfreq race, df(tf1)
frame tf1: list, clean noobs

// * table by marital status
frame nlsw88: dtfreq race, df(tf2) by(married)
frame tf2: list, noobs sepby(varname)

// * table across marital status
frame nlsw88: dtfreq race, df(tf3) cross(married)
frame tf3: list, clean noobs

// * table by college graduation and across marital status
frame nlsw88: dtfreq race, df(tf4) by(collgrad) cross(married)
frame tf4: describe

// * binary output
frame nlsw88: dtfreq union, df(tf5) binary
frame tf5: list, clean noobs

// * binary output with missing values excluded from the analysis sample
frame nlsw88: dtfreq union, df(tf6) binary nomiss
frame tf6: list, clean noobs

// * binary output by college graduation and across race
frame nlsw88: dtfreq union, df(tf7) by(collgrad) cross(race) ///
    binary format(%8.2f)
frame tf7: describe

exit, clear
