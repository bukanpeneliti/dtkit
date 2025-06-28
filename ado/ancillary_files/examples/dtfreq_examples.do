// * examples:
// Setup using a standard Stata dataset:

    . capture frame create nlsw88
    . frame nlsw88: sysuse nlsw88.dta, clear

// 1. One-way frequency table for race, results in frame tf1:

    . frame nlsw88: dtfreq race, df(tf1)
    . frame tf1: list, clean noobs

// 2. Frequency table for race stratified by married (rows), results in frame tf2:

    . frame nlsw88: dtfreq race, df(tf2) rowby(married)
    . frame tf2: list, noobs sepby(varname)

// 3. Frequency table for race cross-tabulated by married (columns), results in frame tf3:

    . frame nlsw88: dtfreq race, df(tf3) colby(married)
    . frame tf3: list, clean noobs

// 4. Two-way table for race, rows by collgrad, columns by married, results in frame tf4:

    . frame nlsw88: dtfreq race, df(tf4) rowby(collgrad) colby(married)
    . frame tf4: describe

// 5. Using yesno for the binary variable union. Results in frame tf5. This will produce incorrect proportions/percentages, because union has missing values:

    . frame nlsw88: dtfreq union, df(tf5) yesno
    . frame tf5: list, clean noobs

// 6. Using yesno with nomiss. Results in frame tf6:

    . frame nlsw88: dtfreq union, df(tf6) yesno nomiss
    . frame tf6: list, clean noobs

// 7. Using yesno with colby and formatting option. Results in frame tf7:

    . frame nlsw88: dtfreq union, df(tf7) rowby(collgrad) colby(race) yesno format(%8.2f)
    . frame tf7: describe
