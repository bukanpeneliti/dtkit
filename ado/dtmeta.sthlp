{smcl}
Program Version: {bf:1.0.1} (25 Jun 2025){vieweralsosee "[R] notes" "help notes"}{...}
{vieweralsosee "[R] label" "help label"}{...}
{vieweralsosee "[D] frames" "help frames"}{...}
{viewerjumpto "Syntax" "dtmeta##syntax"}{...}
{viewerjumpto "Description" "dtmeta##description"}{...}
{viewerjumpto "Options" "dtmeta##options"}{...}
{viewerjumpto "Remarks" "dtmeta##remarks"}{...}
{viewerjumpto "Examples" "dtmeta##examples"}{...}
{viewerjumpto "Stored results" "dtmeta##results"}{...}
{viewerjumpto "Author" "dtmeta##author"}{...}
{title:Title}

{phang}
{bf:dtmeta} {hline 2} Extract dataset metadata into multiple frames

{marker syntax}{...}
{title:Syntax}

{p 8 17 2}
{cmdab:dtmeta}
[{cmd:using} {it:{help filename}}]
[{cmd:,} {it:options}]

{synoptset 20 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Main}
{synopt:{opt c:lear}}clear original data from memory after loading external data{p_end}
{synopt:{opt rep:lace}}replace existing metadata frames{p_end}
{synopt:{opt report}}display metadata extraction report{p_end}
{synopt:{opt save(filename)}}save metadata to Excel file{p_end}
{synoptline}
{p2colreset}{...}

{marker description}{...}
{title:Description}

{pstd}
{cmd:dtmeta} extracts comprehensive metadata from a Stata dataset and organizes it into 
separate {help frame:frames} for easy analysis and documentation. The command creates up to four frames,
each containing different aspects of the dataset's metadata:

{phang2}o Variable metadata ({cmd:_dtvars}){p_end}
{phang2}o Value label metadata ({cmd:_dtlabel}){p_end}
{phang2}o Variable notes ({cmd:_dtnotes}){p_end}
{phang2}o Dataset information and characteristics ({cmd:_dtinfo}){p_end}
{pstd}This option provides immediate feedback on the metadata extraction process.

{phang}
{opt save(excelname)} exports all created metadata frames to an Excel file named {it:excelname}.
Each frame is saved as a separate worksheet within the Excel file. The worksheet names will
correspond to the frame names (e.g., _dtvars, _dtlabel). If the specified Excel file
already exists, the {opt replace} option must also be used to overwrite it. The filename
can be specified with or without the {cmd:.xlsx} extension.

{marker remarks}{...}
{title:Remarks}

{pstd}
{cmd:dtmeta} facilitates dataset documentation by systematically extracting metadata into
Stata frames. This organization allows for easier review and programmatic access to
dataset characteristics.

{pstd}
{ul:{bf:Frame Structure}}

{pstd}
All frames created by {cmd:dtmeta} include a variable named {cmd:_level}. This variable
contains a string that identifies the type or level of metadata contained in each row
(e.g., "variable" for rows in {cmd:_dtvars}, "value label" for rows in {cmd:_dtlabel}).
Additionally, each frame is assigned a descriptive frame label.

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
{p2col :{cmd:vallab}}Name of the value label set associated with the variable, if any{p_end}
{p2col :{cmd:varlab}}Variable label{p_end}
{p2colreset}{...}

{pstd}
The {cmd:_dtlabel} frame contains detailed information about value labels:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "value label"){p_end}
{p2col :{cmd:varname}}Name of a variable that uses {cmd:vallab}{p_end}
{p2col :{cmd:index}}Order/index of the specific labeled value within {cmd:vallab}{p_end}
{p2col :{cmd:vallab}}Name of the value label set{p_end}
{p2col :{cmd:value}}The numeric value being labeled{p_end}
{p2col :{cmd:label}}The text of the label corresponding to {cmd:value}{p_end}
{p2col :{cmd:trunc}}Indicator for truncated label text (1 if truncated, 0 otherwise){p_end}
{p2colreset}{...}

{pstd}
The {cmd:_dtnotes} frame contains notes attached to variables:

{p2colset 5 25 29 2}{...}
{p2col :{cmd:_level}}Metadata level identifier (e.g., "variable"){p_end}
{p2col :{cmd:varname}}Name of the variable to which the note is attached{p_end}
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
{p2col :{cmd:dta_ts}}Timestamp of when the dataset was last saved{p_end}
{p2colreset}{...}

{pstd}
{ul:{bf:Frame Management}}

{pstd}
Each time {cmd:dtmeta} is executed, it replaces any existing frames named {cmd:_dtvars},
{cmd:_dtlabel}, {cmd:_dtnotes}, or {cmd:_dtinfo}. This ensures that the metadata frames
always reflect the current state of the source dataset as of the last execution of {cmd:dtmeta}.

{pstd}
{ul:{bf:Excel Export}}

{pstd}
When the {cmd:save(excelname)} option is specified, {cmd:dtmeta} exports all created
metadata frames to separate worksheets within the specified Excel file. If an Excel file
with the same name already exists, the {cmd:replace} option must also be specified to
overwrite the existing file. Otherwise, an error will occur.

{pstd}
{ul:{bf:Empty Frames}}

{pstd}
If the source dataset does not contain any variable notes, the {cmd:_dtnotes} frame will not
be created. Similarly, if the dataset has no defined value labels, the {cmd:_dtlabel} frame
will not be created. A message is displayed in the console if these frames are not created due to
the absence of corresponding metadata. The {cmd:_dtvars} and {cmd:_dtinfo} frames are always
created, as datasets will always have variables and basic descriptive characteristics.

{pstd}
{ul:{bf:Reporting and Navigation}}

{pstd}
Upon completion, {cmd:dtmeta} displays {help Stata_commands##clickable_links:clickable links} in the Stata Results window that allow easy access to view the contents of
the created metadata frames (e.g., by executing {cmd:frame _dtvars: list}). If the
{cmd:report} option is specified, a more detailed summary of the extraction process and
the created frames is displayed.

{pstd}
{ul:{bf:Data Preservation}}

{pstd}
When {cmd:dtmeta} processes the dataset currently in memory (i.e., {cmd:using} is not specified),
the dataset in memory is preserved. If {cmd:using} is specified, the dataset in memory
is also preserved unless the {cmd:clear} option is additionally specified, in which case
the data in memory is replaced by the data from the specified file before metadata extraction.

{marker examples}{...}
{title:Examples}

{pstd}Setup using standard Stata datasets:{p_end}

        {cmd:. sysuse auto, clear}

{pstd}Basic metadata extraction examples:{p_end}

{pstd}1. Basic metadata extraction from data in memory:{p_end}

        {cmd:. dtmeta}
        {cmd:. frame _dtvars: list varname type format vallab, clean noobs}
        {cmd:. frame _dtinfo: list, clean noobs}

{pstd}2. Extract metadata from external file:{p_end}

        {cmd:. dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta"}
        {cmd:. frame _dtvars: list varname type varlab, clean noobs}

{pstd}3. Show detailed report with frame access commands:{p_end}

        {cmd:. dtmeta, report}

{pstd}4. Export to Excel with file replacement:{p_end}

        {cmd:. dtmeta, save(dataset_metadata.xlsx) replace}

{pstd}Advanced examples:{p_end}

{pstd}5. Work with variable metadata:{p_end}

        {cmd:. dtmeta using "https://www.stata-press.com/data/r18/fullauto.dta", clear}
        {cmd:. frame _dtvars: list varname type format vallab}

{pstd}6. Analyze value label coverage:{p_end}

        {cmd:. dtmeta}
        {cmd:. frame _dtvars: generate has_vallab = (vallab != "")}
        {cmd:. frame _dtvars: tab has_vallab}

{pstd}7. Examine variable notes:{p_end}

        {cmd:. notes make: test note}
        {cmd:. dtmeta}
        {cmd:. frame _dtnotes: list varname _note_text}

{pstd}8. Review dataset information:{p_end}

        {cmd:. dtmeta}
        {cmd:. frame _dtinfo: list, noobs}

{pstd}9. Comprehensive workflow with external data and export:{p_end}

        {cmd:. dtmeta using "https://www.stata-press.com/data/r18/nlswork.dta", save(mydata_metadata.xlsx) replace report clear}

{marker results}{...}
{title:Stored results}

{pstd}
{cmd:dtmeta} stores the following in {cmd:r()}:

{synoptset 18 tabbed}{...}
{synopthdr:Scalars}
{synoptline}
{p2col:{cmd:r(N)}}number of observations{p_end}
{p2col:{cmd:r(k)}}number of variables{p_end}
{synoptline}
{p2colreset}{...}

{synoptset 18 tabbed}{...}
{synopthdr:Macros}
{synoptline}
{p2col:{cmd:r(varlist)}}variable names{p_end}
{p2col:{cmd:r(source_frame)}}name of source data frame{p_end}
{synoptline}
{p2colreset}{...}

{pstd}
{cmd:dtmeta} creates the following frames:

{synoptset 18 tabbed}{...}
{synopthdr:Frames}
{synoptline}
{p2col:{cmd:_dtvars}}Variable metadata (always created){p_end}
{p2col:{cmd:_dtlabel}}Value label metadata (if value labels exist){p_end}
{p2col:{cmd:_dtnotes}}Variable notes (if variable notes exist){p_end}
{p2col:{cmd:_dtinfo}}Dataset information and notes (always created){p_end}
{synoptline}
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
Manual: {manlink R describe}, {manlink R notes}, {manlink R label}, {manlink D frames}

{psee}
Online: {helpb describe}, {helpb notes}, {helpb notes_}, {helpb label}, {helpb frames}
