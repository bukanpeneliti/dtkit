// * examples
// Setup using a standard Stata dataset:
        
        . capture frame create nlsw88
        . frame nlsw88: sysuse nlsw88.dta, clear

// 1. One-way descriptive statistics for {cmd:age} and {cmd:grade}, results in frame {cmd:_df}:

        . frame nlsw88: dtstat age grade
        . frame _df: list, clean noobs

// 2. Descriptive statistics (default) for {cmd:age} and {cmd:grade} stratified by {cmd:married}, results in frame {cmd:df2}:

        . frame nlsw88: dtstat age grade, df(df2) by(married)
        . frame df2: list, noobs sepby(married)

// 3. Descriptive statistics (count, mean, and standard deviation) for {cmd:age} and {cmd:grade} stratified by {cmd:married}, results in frame {cmd:df3}:

        . frame nlsw88: dtstat age grade, df(df3) by(married) stats(count mean sd)
        . frame df3: list, noobs sepby(married)

// 4. Descriptive statistics (count, mean, and standard deviation) for {cmd:age} and {cmd:grade} stratified by {cmd:married} with {opt format} option (i.e. non-integer values are displayed with two decimal digit and both integer), results in frame {cmd:df4}:

        . frame nlsw88: dtstat age grade, df(df4) by(married) stats(count mean sd) format(%8.2f)
        . frame df4: list, noobs sepby(married)

// 5. One-way descriptive statistics for {cmd:age} and {cmd:grade}, results in frame {cmd:_df}, export to excel:

        . frame nlsw88: dtstat age grade using "examples/_df"
        . frame _df: list, clean noobs

// 6. Descriptive statistics (default) for {cmd:age} and {cmd:grade} stratified by {cmd:married}, results in frame {cmd:df2}, export to excel:

        . frame nlsw88: dtstat age grade, df(df2) by(married) save("examples/df2.xlsx") excel(sheet("sum", modify))
        . frame df2: list, noobs sepby(married)
