{smcl}
{* *! version 1.0.0  14jan2026}{...}
{vieweralsosee "[D] use" "help use"}{...}
{vieweralsosee "[D] save" "help save"}{...}
{vieweralsosee "[D] import" "help import"}{...}
{vieweralsosee "[D] export" "help export"}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "frames" "help frames"}{...}
{vieweralsosee "python" "help python"}{...}
{viewerjumpto "Syntax" "dtparquet##syntax"}{...}
{viewerjumpto "Description" "dtparquet##description"}{...}
{viewerjumpto "Links to PDF documentation" "dtparquet##linkspdf"}{...}
{viewerjumpto "Options" "dtparquet##options"}{...}
{viewerjumpto "Examples" "dtparquet##examples"}{...}
{viewerjumpto "Author" "dtparquet##author"}{...}
{p2colset 1 21 23 2}{...}
{p2col:{bf:[D] dtparquet} {hline 2}}High-performance Parquet I/O using Python/Arrow{p_end}
{p2col:}({mansection D dtparquet:View complete PDF manual entry}){p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{pstd}
Memory operations

{p 8 16 2}
{cmd:dtparquet} {opt sa:ve}
{it:{help filename}}
[{cmd:,} {it:save_options}]

{p 8 16 2}
{cmd:dtparquet} {opt u:se}
[{varlist}]
[{ifin}]
{cmd:using} {it:{help filename}}
[{cmd:,} {it:use_options}]

{pstd}
Disk operations (no data loaded into active memory)

{p 8 16 2}
{cmd:dtparquet} {opt exp:ort}
{it:{help filename:parquetfile}}
{cmd:using} {it:{help filename:dtafile}}
[{cmd:,} {it:export_options}]

{p 8 16 2}
{cmd:dtparquet} {opt imp:ort}
{it:{help filename:dtafile}}
{cmd:using} {it:{help filename:parquetfile}}
[{cmd:,} {it:import_options}]


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtparquet} provides high-performance reading and writing of Apache Parquet
files.  It serves as a bridge between Stata and the Parquet ecosystem using
Python's {cmd:pyarrow} library and Stata's {help sfi:Stata Function Interface (SFI)}.

{pstd}
Key features include:

{phang2}
o {bf:Metadata Preservation}: Automatically integrates with {help dtmeta} to
store and restore value labels, variable labels, and notes within the Parquet
file footer.

{phang2}
o {bf:Memory Efficiency}: Uses a streaming architecture for disk-to-disk
operations ({cmd:import} and {cmd:export}), allowing processing of datasets
larger than available RAM.

{phang2}
o {bf:Atomic Safety}: Disk operations utilize temporary files ({it:.tmp}) to
ensure that the target file is only created or updated if the entire operation
succeeds.

{phang2}
o {bf:Type Safety}: Handles complex Stata types including {it:strL} and provides
options to handle precision limits of 64-bit integers.


{marker linkspdf}{...}
{title:Links to PDF documentation}

{pstd}
No PDF documentation is available for this user-written command.


{marker options}{...}
{title:Options}

{pstd}
Options are presented under the following headings:

{phang2}
{help dtparquet##save_options:Options for dtparquet save}{p_end}
{phang2}
{help dtparquet##use_options:Options for dtparquet use}{p_end}
{phang2}
{help dtparquet##export_options:Options for dtparquet export}{p_end}
{phang2}
{help dtparquet##import_options:Options for dtparquet import}{p_end}

{marker save_options}{...}
{dlgtab:Options for dtparquet save}

{synoptset 26 tabbed}{...}
{synopthdr :save_options}
{synoptline}
{synopt :{opt re:place}}overwrite existing file{p_end}
{synopt :{opt nol:abel}}suppress writing custom Stata metadata (value labels, etc.){p_end}
{synopt :{opt ch:unksize(#)}}batch size for processing; default is 50,000{p_end}
{synoptline}

{marker use_options}{...}
{dlgtab:Options for dtparquet use}

{synoptset 26 tabbed}{...}
{synopthdr :use_options}
{synoptline}
{synopt :{opt c:lear}}clear data in memory before loading{p_end}
{synopt :{opt nol:abel}}suppress reading custom Stata metadata{p_end}
{synopt :{opt ch:unksize(#)}}batch size for processing; default is 50,000{p_end}
{synopt :{opt all:string}}import 64-bit integers as strings to preserve precision{p_end}
{synoptline}

{marker export_options}{...}
{dlgtab:Options for dtparquet export}

{synoptset 26 tabbed}{...}
{synopthdr :export_options}
{synoptline}
{synopt :{opt re:place}}overwrite existing file{p_end}
{synopt :{opt nol:abel}}suppress writing custom Stata metadata{p_end}
{synopt :{opt ch:unksize(#)}}batch size for processing; default is 50,000{p_end}
{synoptline}

{marker import_options}{...}
{dlgtab:Options for dtparquet import}

{synoptset 26 tabbed}{...}
{synopthdr :import_options}
{synoptline}
{synopt :{opt re:place}}overwrite existing file{p_end}
{synopt :{opt nol:abel}}suppress reading custom Stata metadata{p_end}
{synopt :{opt ch:unksize(#)}}batch size for processing; default is 50,000{p_end}
{synopt :{opt all:string}}import 64-bit integers as strings to preserve precision{p_end}
{synoptline}


{marker examples}{...}
{title:Examples}

{pstd}Save the current dataset to Parquet (abbreviated){p_end}
{phang2}{cmd:. dtparquet sa mydata.parquet, re}

{pstd}Load specific variables from a Parquet file (abbreviated){p_end}
{phang2}{cmd:. dtparquet u id price mpg using mydata.parquet, c}

{pstd}Import large IDs from a foreign Parquet file as strings{p_end}
{phang2}{cmd:. dtparquet use using big_ids.parquet, allstring clear}

{pstd}Export a large .dta file to Parquet without loading it into memory{p_end}
{phang2}{cmd:. dtparquet export results.parquet using raw_data.dta, replace}

{pstd}Convert a Parquet file to Stata format on disk{p_end}
{phang2}{cmd:. dtparquet import final.dta using results.parquet, replace allstring}


{marker author}{...}
{title:Author}

{pstd}
Hafiz Arfyanto
{p_end}
{pstd}
Email: {browse "mailto:hafizarfyanto@gmail.com":hafizarfyanto@gmail.com}
{p_end}
{pstd}
GitHub: {browse "https://github.com/hafizarfyanto/dtkit":https://github.com/hafizarfyanto/dtkit}

{pstd}
For questions and suggestions, visit {browse "https://github.com/hafizarfyanto/dtkit/issues":GitHub Issues}.

{marker also_see}{...}
{title:Also see}

{psee}
Manual: {manlink D use}, {manlink D save}, {manlink D import}, {manlink D export}

{psee}
Online: {helpb use}, {helpb save}, {helpb import}, {helpb export}, {helpb dtmeta}, {helpb frames}
