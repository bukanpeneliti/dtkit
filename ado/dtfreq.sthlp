{smcl}
{* *! version 1.0.2 25Jun2025}{...}
{vieweralsosee "[R] contract" "help contract"}{...}
{vieweralsosee "[R] table" "help table"}{...}
{vieweralsosee "[R] tabstat" "help tabstat"}{...}
{vieweralsosee "[R] tabulate" "help tabulate"}{...}
{viewerjumpto "Syntax" "dtfreq##syntax"}{...}
{viewerjumpto "Description" "dtfreq##description"}{...}
{viewerjumpto "Options" "dtfreq##options"}{...}
{viewerjumpto "Examples" "dtfreq##examples"}{...}
{viewerjumpto "Stored results" "dtfreq##results"}{...}
{viewerjumpto "Author" "dtfreq##author"}{...}
{title:Title}

{phang}
{bf:dtfreq} {hline 2} Produce comprehensive frequency datasets

{marker syntax}{...}
{title:Syntax}

{p 8 17 2}
{cmdab:dtfreq}
{varlist}
{ifin}
{weight}
[{cmd:using} {it:filename}]
[{cmd:,} {it:options}]

{synoptset 24 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Main}
{synopt:{opt df(framename)}}specify name for destination {help frame}; default is {cmd:_df}{p_end}
{synopt:{opt by}({varname})}create frequency tables by row groups{p_end}
{synopt:{opt cross}({varname})}create frequency tables by column groups{p_end}
{synopt:{opt bin:ary}}reshape binary variables for yes/no analysis{p_end}
{synopt:{opt c:lear}}clear data from memory when using external file{p_end}

{syntab:Statistics}
{synopt:{opt stat:s(statlist)}}specify statistics direction: {cmdab:row}, {cmdab:col}, {cmdab:cell}; default is {cmd:col}{p_end}
{synopt:{opt ty:pe(typelist)}}specify statistics type: {cmdab:prop}, {cmdab:pct}; default is {cmd:prop}{p_end}

{syntab:Display}
{synopt:{opt fo:rmat(%fmt)}}specify display format for numeric variables{p_end}
{synopt:{opt nomiss}}exclude missing values from analysis{p_end}

{syntab:Export}
{synopt:{opt save(excelname)}}export results to Excel file{p_end}
{synopt:{opt excel(export_options)}}additional options for Excel export{p_end}
{synopt:{opt rep:lace}}replace existing Excel file when saving{p_end}
{synoptline}
{p2colreset}{...}
{p 4 6 2}
{cmd:aweight}s, {cmd:fweight}s, {cmd:iweight}s, and {cmd:pweight}s are allowed; see {help weight}.{p_end}

{marker description}{...}
{title:Description}

{pstd}
{cmd:dtfreq} produces comprehensive frequency datasets from one or more numeric variables. 
The command creates detailed frequency tables with counts, proportions, and percentages, 
optionally organized by row and column groupings. Results are stored in a new {help frame}
and can be exported to Excel format.

{pstd}
Unlike basic frequency commands (e.g., {help tabulate} or {help contract}), {cmd:dtfreq} provides:

{phang2}o Processing of multiple variables simultaneously.{p_end}
{phang2}o Cross-tabulation capabilities, creating tables with row and column groupings defined by variables.{p_end}
{phang2}o Flexible statistics calculation (row, column, or cell proportions/percentages).{p_end}
{phang2}o Automatic calculation of totals for groups and overall.{p_end}
{phang2}o Preservation and display of value labels in the output dataset.{p_end}
{phang2}o Binary variable reshaping, which structures variables with yes/no type responses into separate columns for each category.{p_end}
{phang2}o Direct Excel export functionality for the resulting dataset.{p_end}
{pstd}Variable presence depends on {opt stats()} and {opt type()} options:{p_end}

{phang2}o {opt stats(col)} creates {cmd:colprop*} and/or {cmd:colpct*} variables{p_end}
{phang2}o {opt stats(row)} creates {cmd:rowprop*} and/or {cmd:rowpct*} variables{p_end}
{phang2}o {opt stats(cell)} creates {cmd:cellprop*} and/or {cmd:cellpct*} variables{p_end}
{phang2}o {opt type(prop)} includes proportion variables{p_end}
{phang2}o {opt type(pct)} includes percentage variables{p_end}


{pstd}When {opt cross}({it:varname}) is specified, the output variables representing frequencies and statistics are reshaped wide. This means that for each category of the {it:varname} specified in {opt cross()}, a new set of variables is created, typically with numeric suffixes (e.g., {cmd:freq_1}, {cmd:freq_2}, {cmd:colprop_1}, {cmd:colprop_2}) appended to the base variable names to distinguish the column groups.{p_end}
{pstd}When {opt binary} is specified, variables in the output dataset are structured to represent the different response categories of the binary input variable. This often involves creating prefixed variable names (e.g., {cmd:yes_variablename}, {cmd:no_variablename}) or similar structures to clearly indicate each response category within the reshaped data.{p_end}

{pstd}
The active frame remains unchanged unless an error occurs during frame switching.

{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}{p_end}

{pstd}
For questions and suggestions, visit {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues}.

{title:Also see}

{pstd}{helpb tabulate}, {helpb contract}, {helpb table}, {helpb tabstat}{p_end}