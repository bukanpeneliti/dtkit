{smcl}
{* *! version 1.0.2  25jun2025}{...}
{vieweralsosee "[R] contract" "help contract"}{...}
{vieweralsosee "[R] table" "help table"}{...}
{vieweralsosee "[R] tabstat" "help tabstat"}{...}
{vieweralsosee "[R] tabulate" "help tabulate"}{...}
{vieweralsosee "" "--"}{...}
{vieweralsosee "dtstat" "help dtstat"}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "dtparquet" "help dtparquet"}{...}
{viewerjumpto "Syntax" "dtfreq##syntax"}{...}
{viewerjumpto "Description" "dtfreq##description"}{...}
{viewerjumpto "Links to PDF documentation" "dtfreq##linkspdf"}{...}
{viewerjumpto "Options" "dtfreq##options"}{...}
{viewerjumpto "Examples" "dtfreq##examples"}{...}
{viewerjumpto "Stored results" "dtfreq##results"}{...}
{viewerjumpto "Author" "dtfreq##author"}{...}
{viewerjumpto "Also see" "dtfreq##also_see"}{...}
{p2colset 1 16 18 2}{...}
{p2col:{bf:[D] dtfreq} {hline 2}}Produce comprehensive frequency datasets{p_end}
{p2col:}({mansection D dtfreq:View complete PDF manual entry}){p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{p 8 16 2}
{cmd:dtfreq}
{varlist}
[{ifin}]
[{weight}]
[{cmd:using} {it:{help filename}}]
[{cmd:,} {it:options}]

{synoptset 24 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Main}
{synopt :{opt df(framename)}}specify name for destination {help frame}; default is {cmd:_df}{p_end}
{synopt :{opt by}({varname})}create frequency tables by row groups{p_end}
{synopt :{opt cross}({varname})}create frequency tables by column groups{p_end}
{synopt :{opt bin:ary}}reshape binary variables for yes/no analysis{p_end}
{synopt :{opt c:lear}}clear data from memory when using external file{p_end}

{syntab:Statistics}
{synopt :{opt stat:s(statlist)}}specify statistics direction: {cmd:row}, {cmd:col}, {cmd:cell}; default is {cmd:col}{p_end}
{synopt :{opt ty:pe(typelist)}}specify statistics type: {cmd:prop}, {cmd:pct}; default is {cmd:prop}{p_end}

{syntab:Display}
{synopt :{opt fo:rmat(%fmt)}}specify display format for numeric variables{p_end}
{synopt :{opt nomiss}}exclude missing values from analysis{p_end}

{syntab:Export}
{synopt :{opt save(excelname)}}export results to Excel file{p_end}
{synopt :{opt excel(export_options)}}additional options for Excel export{p_end}
{synopt :{opt rep:lace}}replace existing Excel file when saving{p_end}
{synoptline}
{p 4 6 2}
{opt aweight}s, {opt fweight}s, {opt iweight}s, and {opt pweight}s are allowed; see {help weight}.{p_end}


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtfreq} produces comprehensive frequency datasets from one or more numeric variables. 
The command creates detailed frequency tables with counts, proportions, and percentages, 
optionally organized by row and column groupings.  {cmd:dtfreq} stores results in a new {help frame}.
The command also exports results to Excel format.

{pstd}
{cmd:dtfreq} includes several advanced features for data analysis:

{phang2}o Processing of multiple variables simultaneously.{p_end}
{phang2}o Cross-tabulation capabilities, creating tables with row and column groupings defined by variables.{p_end}
{phang2}o Flexible statistics calculation (row, column, or cell proportions/percentages).{p_end}
{phang2}o Automatic calculation of totals for groups and overall.{p_end}
{phang2}o Preservation and display of value labels in the output dataset.{p_end}
{phang2}o Binary variable reshaping, which structures variables with yes/no type responses into separate columns for each category.{p_end}
{phang2}o Direct Excel export functionality for the resulting dataset.{p_end}

{pstd}
{opt stats()} and {opt type()} options determine the variables in the output dataset.
The following list describes the resulting variable names:

{phang2}o {opt stats(col)} creates {cmd:colprop*} and/or {cmd:colpct*} variables{p_end}
{phang2}o {opt stats(row)} creates {cmd:rowprop*} and/or {cmd:rowpct*} variables{p_end}
{phang2}o {opt stats(cell)} creates {cmd:cellprop*} and/or {cmd:cellpct*} variables{p_end}
{phang2}o {opt type(prop)} includes proportion variables{p_end}
{phang2}o {opt type(pct)} includes percentage variables{p_end}


{marker linkspdf}{...}
{title:Links to PDF documentation}

{pstd}
No PDF documentation is available for this user-written command.


{marker options}{...}
{title:Options}

{dlgtab:Main}

{phang}
{opt df(framename)} specifies the {help frame} name for the output dataset.
{cmd:dtfreq} uses frame {cmd:_df} if {it:framename} is omitted.
The command replaces any existing frame with the same name.

{phang}
{opt by}({varname}) creates frequency tables organized by row groups based on the categories of {it:varname}.
The specified variable defines these row groupings.
{cmd:dtfreq} calculates totals for each group.
The command adds an overall "Total" row to the output frame.

{phang}
{opt cross}({varname}) creates frequency tables with column groups based on the categories of {it:varname}.
The specified variable defines this column structure, creating separate sets of frequency, proportion,
and percentage columns for each of its values.
Do not specify the same variable in both {opt cross()} and {opt by()}.

{phang}
{opt binary} reshapes the output for binary variables (variables with only two distinct nonmissing values, typically representing yes/no, true/false, or 0/1).
This option creates separate columns in the output dataset for each response category of the binary variable.
When combined with {opt cross}, this creates complex output structures.

{phang}
{opt clear} clears data in the current {help frame}.
This allows {cmd:dtfreq} to load the external file from {cmd:using}.

{dlgtab:Statistics}

{phang}
{opt stats(statlist)} sets the direction for calculations.
Valid values include the following:

{phang2}{cmd:col} - calculate column proportions/percentages (default).  The calculation uses column totals as denominators.{p_end}
{phang2}{cmd:row} - calculate row proportions/percentages.  The calculation uses row totals as denominators.{p_end}
{phang2}{cmd:cell} - calculate cell proportions/percentages.  The calculation uses the overall total count as the denominator.{p_end}

{pmore}Specify multiple options to calculate several statistics.
This option is effective when {opt cross()} is also specified.

{phang}
{opt type(typelist)} specifies the statistics types for the output dataset.
Users choose proportions, percentages, or both.

{phang2}{cmd:prop} - display proportions (scaled from 0 to 1; default).{p_end}
{phang2}{cmd:pct} - display percentages (scaled from 0 to 100).{p_end}

{pmore}Specify both types to include proportions and percentages.

{dlgtab:Display}

{phang}
{opt format(%fmt)} specifies the {helpb format:display format} for all numeric statistic variables
(frequencies, proportions, percentages) in the output dataset.
If not specified, {cmd:dtfreq} automatically applies suitable formats: {cmd:%20.0fc} for counts, {cmd:%6.3fc} for proportions (0-1),
and {cmd:%20.1fc} for percentages (0-100) and other decimal numbers.
The format must strictly follow Stata's {helpb format:formatting rules}.

{phang}
{opt nomiss} excludes observations with missing values in any of the {varlist} variables from all calculations.
{cmd:dtfreq} treats missing values in {varlist} as a distinct category by default.
{opt nomiss} restricts proportion and percentage calculations to nonmissing observations.

{dlgtab:Export}

{phang}
{opt save(excelname)} exports results to an Excel file.
{cmd:save()} exports the frequency table frame to an Excel file.
{cmd:dtfreq} stores results only in the Stata frame if {cmd:save()} is omitted.

{phang}
{opt excel(export_options)} specifies additional options for {help export_excel##export_excel_options:Excel export} when using the 
{cmd:save()} option.  These options are passed directly to the {help export_excel:export excel} 
command.  Can only be used with {cmd:save()}.


{marker examples}{...}
{title:Examples}

{pstd}Setup using standard Stata datasets:{p_end}
{phang2}{cmd:. sysuse auto, clear}{p_end}

{pstd}Basic frequency examples:{p_end}

{pstd}1. Simple frequency table for one variable:{p_end}
{phang2}{cmd:. dtfreq rep78}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}

{pstd}2. Multiple variables:{p_end}
{phang2}{cmd:. dtfreq rep78 foreign}{p_end}
{phang2}{cmd:. frame _df: tab varname}{p_end}

{pstd}3. Show percentages instead of proportions:{p_end}
{phang2}{cmd:. dtfreq rep78, type(pct)}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}

{pstd}Cross-tabulation examples:{p_end}

{pstd}4. by option (row groups):{p_end}
{phang2}{cmd:. dtfreq rep78, by(foreign)}{p_end}
{phang2}{cmd:. frame _df: list varname foreign vallab freq colprop, clean sepby(varname)}{p_end}

{pstd}5. cross option (column groups):{p_end}
{phang2}{cmd:. dtfreq rep78, cross(foreign)}{p_end}
{phang2}{cmd:. frame _df: describe, simple}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}

{pstd}6. Both by and cross options:{p_end}
{phang2}{cmd:. dtfreq rep78, by(foreign) cross(trunk)}{p_end}
{phang2}{cmd:. frame _df: list, clean sepby(varname)}{p_end}

{pstd}Export examples:{p_end}

{pstd}7. Export to Excel:{p_end}
{phang2}{cmd:. dtfreq rep78, save(dtfreq_output.xlsx) replace}{p_end}

{pstd}8. Using external data file:{p_end}
{phang2}{cmd:. dtfreq rep78 using "auto.dta", clear}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}


{marker results}{...}
{title:Stored results}

{pstd}
{cmd:dtfreq} stores results in the specified frame (default: {cmd:_df}).  The output dataset contains:

{synoptset 15 tabbed}{...}
{p2col 5 20 24 2: Variables}{p_end}
{synopt :{cmd:varname}}original variable name{p_end}
{synopt :{cmd:varlab}}variable label{p_end}
{synopt :{cmd:vallab}}value labels or string representation{p_end}
{synopt :{cmd:freq*}}frequency counts (with suffixes when using {opt cross}){p_end}

{pstd}Statistics variables (prefixed by direction):{p_end}
{synopt :{cmd:colprop*}}column proportions (default){p_end}
{synopt :{cmd:colpct*}}column percentages{p_end}
{synopt :{cmd:rowprop*}}row proportions{p_end}
{synopt :{cmd:rowpct*}}row percentages{p_end}
{synopt :{cmd:cellprop*}}cell proportions{p_end}
{synopt :{cmd:cellpct*}}cell percentages{p_end}
{synopt :{cmd:*total*}}total counts for denominators{p_end}
{p2colreset}{...}

{pstd}{bf:prop_all} and {bf:pct_all} show the overall proportion (1) and percentage (100) for the total row when using {opt cross()}.  They help identify the grand total in the results.{p_end}

{pstd}
{opt cross}({varname}) reshapes the output statistics into wide format.
{cmd:dtfreq} creates new variables for each category in {opt cross()}.
The command uses numeric suffixes to distinguish column groups.

{pstd}
When {opt binary} is specified, variables in the output dataset are structured to represent the different response categories of the binary input variable.
This involves creating prefixed variable names (e.g., {cmd:yes_variablename}, {cmd:no_variablename}) to indicate each response category within the reshaped data.

{pstd}
The active frame remains unchanged throughout execution.
{cmd:dtfreq} switches frames internally and returns to the original frame upon completion.


{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}{p_end}

{pstd}
The GitHub repository hosts the latest updates and documentation.
Report issues and suggestions at {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues}.


{marker also_see}{...}
{title:Also see}

{psee}
Online: {helpb tabulate}, {helpb contract}, {helpb table}, {helpb tabstat}, {helpb dtstat}, {helpb dtmeta}{p_end}
