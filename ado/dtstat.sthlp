{smcl}
{* *! version 1.0.2  25jun2025}{...}
{vieweralsosee "[R] summarize" "help summarize"}{...}
{vieweralsosee "[R] collapse" "help collapse"}{...}
{vieweralsosee "[R] tabstat" "help tabstat"}{...}
{vieweralsosee "[D] frame" "help frame"}{...}
{vieweralsosee "" "--"}{...}
{vieweralsosee "dtfreq" "help dtfreq"}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "dtparquet" "help dtparquet"}{...}
{viewerjumpto "Syntax" "dtstat##syntax"}{...}
{viewerjumpto "Description" "dtstat##description"}{...}
{viewerjumpto "Links to PDF documentation" "dtstat##linkspdf"}{...}
{viewerjumpto "Options" "dtstat##options"}{...}
{viewerjumpto "Examples" "dtstat##examples"}{...}
{viewerjumpto "Stored results" "dtstat##results"}{...}
{viewerjumpto "Author" "dtstat##author"}{...}
{viewerjumpto "Also see" "dtstat##also_see"}{...}
{p2colset 1 16 18 2}{...}
{p2col:{bf:[D] dtstat} {hline 2}}Produce descriptive statistics dataset{p_end}
{p2col:}({mansection D dtstat:View complete PDF manual entry}){p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{p 8 16 2}
{cmd:dtstat}
{varlist}
[{ifin}]
[{weight}]
[{cmd:using} {it:{help filename}}]
[{cmd:,} {it:options}]

{synoptset 24 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Main}
{synopt :{opt df(framename)}}specify frame name for output dataset; default is {cmd:_df}{p_end}
{synopt :{opt by(varlist)}}produce statistics by groups of variables{p_end}
{synopt :{opt stats(statlist)}}specify statistics to calculate; default is {cmd:count mean median min max}{p_end}
{synopt :{opt fo:rmat(%fmt)}}specify number format for numeric variables{p_end}
{synopt :{opt nomiss}}exclude observations with missing values in variables{p_end}
{synopt :{opt fa:st}}use {cmd:gtools} commands for faster processing{p_end}
{synopt :{opt clear}}clear data from memory when using external file{p_end}

{syntab:Export}
{synopt :{opt save(excelname)}}export results to Excel file{p_end}
{synopt :{opt excel(export_options)}}specify additional options for Excel export{p_end}
{synopt :{opt rep:lace}}replace existing Excel file when saving{p_end}
{synoptline}
{p 4 6 2}
{opt aweight}s, {opt fweight}s, {opt iweight}s, and {opt pweight}s are allowed;
see {help weight}.{p_end}


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtstat} creates a dataset containing descriptive statistics for {varlist}.
{cmd:dtstat} stores results in a Stata {help frame} and exports them to an Excel file with {cmd:save()}.
The command produces a new dataset for further manipulation, merging, or reporting.

{pstd}
{opt by(varlist)} requests statistics for each group.
The output dataset includes rows for each group and additional rows for overall totals.
{cmd:dtstat} preserves value labels for grouping variables and labels total rows "Total".

{pstd}
{cmd:dtstat} leverages {helpb frames} for efficient data management.
{opt fast} utilizes {cmd:gcollapse} from the {cmd:gtools} package for improved performance with large datasets.


{marker linkspdf}{...}
{title:Links to PDF documentation}

{pstd}
No PDF documentation is available for this user-written command.


{marker options}{...}
{title:Options}

{dlgtab:Main}

{phang}
{opt df(framename)} specifies the name of the {help frame} for the output dataset.
The default is {cmd:_df}.
{cmd:dtstat} replaces any existing frame with this name.

{phang}
{opt by(varlist)} computes statistics for groups defined by {it:varlist} values.
The output dataset includes rows for each group and overall totals.
{cmd:dtstat} preserves value labels for {it:by_variables}.
The command identifies total rows using a special value and labels them "Total".

{phang}
{opt stats(statlist)} specifies the statistics.
The default list includes {cmd:count mean median min max}.
{cmd:dtstat} supports any statistic from {help collapse}.
Common statistics include:

{pmore2}
{cmd:count} - number of nonmissing observations{break}
{cmd:mean} - arithmetic mean{break}
{cmd:median} - median (50th percentile){break}
{cmd:min} - minimum value{break}
{cmd:max} - maximum value{break}
{cmd:sd} - standard deviation{break}
{cmd:sum} - sum of values{break}
{cmd:p}{it:##} - ##th percentile (e.g., {cmd:p25} for the 25th percentile, {cmd:p75} for the 75th percentile){break}
{cmd:iqr} - interquartile range (difference between the 75th and 25th percentiles){break}
{cmd:first} - first observation in group{break}
{cmd:last} - last observation in group{break}
{cmd:firstnm} - first nonmissing observation in group{break}
{cmd:lastnm} - last nonmissing observation in group

{phang}
{opt format(%fmt)} specifies the {help format:display format} for numeric variables in the output dataset.
{cmd:dtstat} applies {cmd:%20.0fc} to integer statistics and {cmd:%20.1fc} to decimal statistics by default.

{phang}
{opt nomiss} excludes observations with missing values in {varlist} or {opt by(varlist)}.
{cmd:dtstat} otherwise performs calculations using all nonmissing values for each variable or group individually.

{phang}
{opt fast} utilizes {cmd:gtools} commands for computation.
This option improves performance with large datasets.
{cmd:dtstat} issues a warning and proceeds with standard commands if {cmd:gtools} is missing.

{phang}
{opt clear} removes the current dataset before loading a new one with {cmd:using} in the current {help frame}.

{dlgtab:Export}

{phang}
{opt save(excelname)} exports results to an Excel file.
{cmd:dtstat} stores results only in the Stata frame if the user omits this option.

{phang}
{opt excel(export_options)} passes options directly to {cmd:export excel}.
Default options include {cmd:sheet("dtstat_output", modify)} and {cmd:firstrow(varlabels)}.
{cmd:save()} must accompany this option.

{phang}
{opt replace} overwrites the existing Excel file or sheet.
This option allows updating files without manual deletion.


{marker examples}{...}
{title:Examples}

{pstd}Setup using standard Stata datasets:{p_end}
{phang2}{cmd:. sysuse auto, clear}{p_end}

{pstd}Basic descriptive statistics examples:{p_end}

{pstd}1. Simple descriptive statistics:{p_end}
{phang2}{cmd:. dtstat price mpg weight}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}

{pstd}2. Statistics with grouping:{p_end}
{phang2}{cmd:. dtstat price mpg, by(foreign)}{p_end}
{phang2}{cmd:. frame _df: list, noobs sepby(foreign)}{p_end}

{pstd}Export examples:{p_end}

{pstd}3. Export to Excel:{p_end}
{phang2}{cmd:. dtstat age grade, save(dtstat_output.xlsx) replace}{p_end}
{phang2}{cmd:. frame _df: list, clean noobs}{p_end}

{pstd}4. Export with grouping:{p_end}
{phang2}{cmd:. dtstat age grade, by(married) save(dtstat_grouped.xlsx) excel(sheet("summary", modify)) replace}{p_end}
{phang2}{cmd:. frame _df: list, noobs sepby(married)}{p_end}


{marker results}{...}
{title:Stored results}

{pstd}
{cmd:dtstat} creates an output dataset in the target {help frame}.
The dataset contains the following variables:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Variable Name} {it:Description}{p_end}
{synopt :{cmd:varname}}names of input variables{p_end}
{synopt :{cmd:varlab}}labels of input variables{p_end}
{synopt :{it:by_variables}}group identifiers including "Total" rows{p_end}
{synopt :{it:stat_names}}calculated statistics{p_end}
{p2colreset}{...}

{pstd}
The output dataset structure depends on the use of {opt by()}.
The following rules determine the number of observations.

{phang2}o Without {opt by()}, the dataset contains one observation per variable.
Each row represents summary statistics for one variable.{p_end}

{phang2}o With {opt by()}, the dataset contains observations for each variable and group combination.
The dataset includes additional rows for overall totals.{p_end}


{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}{p_end}

{pstd}
For questions and suggestions, visit {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues}.


{marker also_see}{...}
{title:Also see}

{psee}
Online: {helpb summarize}, {helpb collapse}, {helpb tabstat}, {helpb frame}, {helpb export excel}, {helpb dtfreq}, {helpb dtmeta}, {helpb dtparquet}{p_end}
