{smcl}
{* *! version 1.0.1  25jun2025}{...}
{vieweralsosee "[R] describe" "help describe"}{...}
{vieweralsosee "[R] label" "help label"}{...}
{vieweralsosee "[R] notes" "help notes"}{...}
{vieweralsosee "[D] frames" "help frames"}{...}
{vieweralsosee "" "--"}{...}
{vieweralsosee "dtfreq" "help dtfreq"}{...}
{vieweralsosee "dtstat" "help dtstat"}{...}
{vieweralsosee "dtparquet" "help dtparquet"}{...}
{viewerjumpto "Syntax" "dtmeta##syntax"}{...}
{viewerjumpto "Description" "dtmeta##description"}{...}
{viewerjumpto "Links to PDF documentation" "dtmeta##linkspdf"}{...}
{viewerjumpto "Options" "dtmeta##options"}{...}
{viewerjumpto "Remarks" "dtmeta##remarks"}{...}
{viewerjumpto "Examples" "dtmeta##examples"}{...}
{viewerjumpto "Stored results" "dtmeta##results"}{...}
{viewerjumpto "Author" "dtmeta##author"}{...}
{viewerjumpto "Also see" "dtmeta##also_see"}{...}
{p2colset 1 16 18 2}{...}
{p2col:{bf:[D] dtmeta} {hline 2}}Extract dataset metadata into multiple frames{p_end}
{p2col:}({mansection D dtmeta:View complete PDF manual entry}){p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{p 8 16 2}
{cmd:dtmeta}
[{cmd:using} {it:{help filename}}]
[{cmd:,} {it:options}]

{synoptset 20 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Main}
{synopt :{opt c:lear}}clear original data from memory after loading external data{p_end}
{synopt :{opt rep:lace}}replace existing metadata frames{p_end}
{synopt :{opt report}}display metadata extraction report{p_end}
{synopt :{opt save(excelname)}}save metadata to Excel file{p_end}
{synoptline}


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtmeta} extracts comprehensive metadata from a Stata dataset and organizes it into 
separate {help frame:frames} for easy analysis and documentation.
The command creates up to four frames, each containing different aspects of the dataset's metadata:

{phang2}o Variable metadata ({cmd:_dtvars}){p_end}
{phang2}o Value label metadata ({cmd:_dtlabel}){p_end}
{phang2}o Variable notes ({cmd:_dtnotes}){p_end}
{phang2}o Dataset information and characteristics ({cmd:_dtinfo}){p_end}

{pstd}
{cmd:dtmeta} processes the dataset in memory or reads metadata from an external {cmd:.dta} file via {cmd:using}.
Without {opt clear}, {cmd:dtmeta} leaves the data in memory unchanged when processing an external file.


{marker linkspdf}{...}
{title:Links to PDF documentation}

{pstd}
No PDF documentation is available for this user-written command.


{marker options}{...}
{title:Options}

{dlgtab:Main}

{phang}
{opt clear} works only with {cmd:using}.
It loads data from {it:filename} into memory and replaces the current dataset.

{phang}
{opt replace} overwrites an existing Excel file when {cmd:save()} is specified.
{cmd:dtmeta} requires this option to overwrite an existing file of the same name.
Without {opt replace}, {cmd:dtmeta} attempts to modify existing files.
This option works only when {cmd:save()} is specified.

{phang}
{opt report} displays a summary report in the Results window after extraction.
This report includes:

{phang2}o Information about the source dataset (e.g., filename, variables, observations).{p_end}
{phang2}o A summary of the metadata frames, including the number of rows in each.{p_end}
{phang2}o Clickable links to view each frame (e.g., {stata "frame _dtvars: list"}).{p_end}

{phang}
{opt save(excelname)} exports all metadata frames to an Excel file.
Each frame occupies a separate worksheet named after the frame.
{cmd:dtmeta} requires {opt replace} to overwrite existing files.
The command accepts {it:excelname} with or without the {cmd:.xlsx} extension.


{marker remarks}{...}
{title:Remarks}

{pstd}
{cmd:dtmeta} extracts metadata into Stata frames for documentation.
These frames provide programmatic access to dataset characteristics.

{pstd}
{ul:{bf:Frame Structure}}

{pstd}
All frames created by {cmd:dtmeta} include a variable named {cmd:_level}.
{cmd:_level} identifies the metadata type in each row (e.g., "variable" or "value label").
{cmd:dtmeta} also assigns a descriptive label to each created frame.

{pstd}
{ul:{bf:Frame Contents}}

{pstd}
The {cmd:_dtvars} frame contains variable-level metadata:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "variable"){p_end}
{p2col :{cmd:varname}}Variable name{p_end}
{p2col :{cmd:position}}Position of the variable in the dataset order{p_end}
{p2col :{cmd:type}}Storage type of the variable (e.g., {cmd:int}, {cmd:float}, {cmd:str##}){p_end}
{p2col :{cmd:format}}Display format of the variable (e.g., {cmd:%9.0g}, {cmd:%8.2f}){p_end}
{p2col :{cmd:vallab}}Name of the value label set for the variable{p_end}
{p2col :{cmd:varlab}}Variable label{p_end}
{p2colreset}{...}

{pstd}
The {cmd:_dtlabel} frame contains detailed information about value labels:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "value label"){p_end}
{p2col :{cmd:varname}}Name of a variable that uses {cmd:vallab}{p_end}
{p2col :{cmd:index}}Order/index of the specific labeled value within {cmd:vallab}{p_end}
{p2col :{cmd:vallab}}Name of the value label set{p_end}
{p2col :{cmd:value}}The numeric value for the label{p_end}
{p2col :{cmd:label}}The text of the label corresponding to {cmd:value}{p_end}
{p2col :{cmd:trunc}}Indicator for truncated label text (1 if truncated, 0 otherwise){p_end}
{p2colreset}{...}

{pstd}
The {cmd:_dtnotes} frame contains notes attached to variables:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "variable"){p_end}
{p2col :{cmd:varname}}Name of the variable for this note{p_end}
{p2col :{cmd:_note_id}}Sequence number of the note for the variable{p_end}
{p2col :{cmd:_note_text}}Full text content of the note (strL){p_end}
{p2colreset}{...}

{pstd}
The {cmd:_dtinfo} frame contains dataset-level information and notes:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "dataset"){p_end}
{p2col :{cmd:dta_note_id}}Sequence number of a dataset-level note{p_end}
{p2col :{cmd:dta_note}}Full text content of a dataset-level note (strL){p_end}
{p2col :{cmd:dta_obs}}Number of observations in the dataset{p_end}
{p2col :{cmd:dta_vars}}Number of variables in the dataset{p_end}
{p2col :{cmd:dta_label}}Dataset label{p_end}
{p2col :{cmd:dta_ts}}Timestamp of the last dataset save{p_end}
{p2colreset}{...}

{pstd}
{ul:{bf:Frame Management}}

{pstd}
Each time {cmd:dtmeta} executes, it replaces any existing frames named {cmd:_dtvars}, {cmd:_dtlabel}, {cmd:_dtnotes}, or {cmd:_dtinfo}.
Metadata frames always reflect the current state of the source dataset.

{pstd}
{ul:{bf:Excel Export}}

{pstd}
{opt save(excelname)} exports all metadata frames to an Excel file.
{cmd:dtmeta} requires {opt replace} to overwrite an existing file.
Without {opt replace}, the command results in an error if the file exists.

{pstd}
{ul:{bf:Empty Frames}}

{pstd}
{cmd:dtmeta} skips {cmd:_dtnotes} and {cmd:_dtlabel} frames if the dataset lacks notes or value labels.
The command displays a message when it omits these frames.
{cmd:dtmeta} always creates {cmd:_dtvars} and {cmd:_dtinfo} frames because datasets always contain variables and basic characteristics.

{pstd}
{ul:{bf:Reporting and Navigation}}

{pstd}
{cmd:dtmeta} displays {help Stata_commands##clickable_links:clickable links} to the created frames in the Results window.
{opt report} provides a detailed summary of the extraction process and frame contents.

{pstd}
{ul:{bf:Data Preservation}}

{pstd}
{cmd:dtmeta} preserves the dataset in memory when processing it directly.
Without {opt clear}, {cmd:dtmeta} also preserves the current dataset when reading an external file.
{opt clear} replaces the data in memory with the contents of the external file before extraction.


{marker examples}{...}
{title:Examples}

{pstd}Setup using standard Stata datasets:{p_end}
{phang2}{cmd:. sysuse auto, clear}{p_end}

{pstd}Basic metadata extraction examples:{p_end}

{pstd}1. Basic metadata extraction from data in memory:{p_end}
{phang2}{cmd:. dtmeta}{p_end}
{phang2}{cmd:. frame _dtvars: list varname type format vallab, clean noobs}{p_end}
{phang2}{cmd:. frame _dtinfo: list, clean noobs}{p_end}

{pstd}2. Extract metadata from external file:{p_end}
{phang2}{cmd:. dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta"}{p_end}
{phang2}{cmd:. frame _dtvars: list varname type varlab, clean noobs}{p_end}

{pstd}3. Show detailed report with frame access commands:{p_end}
{phang2}{cmd:. dtmeta, report}{p_end}

{pstd}4. Export to Excel with file replacement:{p_end}
{phang2}{cmd:. dtmeta, save(dataset_metadata.xlsx) replace}{p_end}


{marker results}{...}
{title:Stored results}

{pstd}
{cmd:dtmeta} stores the following in {cmd:r()}:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Scalars}{p_end}
{synopt :{cmd:r(N)}}number of observations{p_end}
{synopt :{cmd:r(k)}}number of variables{p_end}

{p2col 5 20 24 2: Macros}{p_end}
{synopt :{cmd:r(varlist)}}variable names{p_end}
{synopt :{cmd:r(source_frame)}}name of source data frame{p_end}
{p2colreset}{...}

{pstd}
{cmd:dtmeta} creates the following frames:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Frames}{p_end}
{synopt :{cmd:_dtvars}}Variable metadata (always created){p_end}
{synopt :{cmd:_dtlabel}}Value label metadata (if value labels exist){p_end}
{synopt :{cmd:_dtnotes}}Variable notes (if variable notes exist){p_end}
{synopt :{cmd:_dtinfo}}Dataset information and notes (always created){p_end}
{p2colreset}{...}


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
Online: {helpb describe}, {helpb notes}, {helpb label}, {helpb frames}, {helpb dtfreq}, {helpb dtstat}, {helpb dtparquet}{p_end}
