{smcl}
{* *! version 1.0.0  07jan2026}{...}
{vieweralsosee "[D] save" "help save"}{...}
{vieweralsosee "[D] use" "help use"}{...}
{vieweralsosee "[D] python" "help python"}{...}
{viewerjumpto "Syntax" "dtparquet##syntax"}{...}
{viewerjumpto "Description" "dtparquet##description"}{...}
{viewerjumpto "Options" "dtparquet##options"}{...}
{viewerjumpto "Remarks" "dtparquet##remarks"}{...}
{viewerjumpto "Examples" "dtparquet##examples"}{...}
{viewerjumpto "Author" "dtparquet##author"}{...}
{title:Title}

{phang}
{bf:dtparquet} {hline 2} Save and load Stata datasets in Parquet format with metadata preservation


{marker syntax}{...}
{title:Syntax}

{pstd}
Save data to Parquet format

{p 8 17 2}
{cmd:dtparquet save} {it:{help filename}} [{it:{help if}}] [{it:{help in}}]
[{cmd:,} {it:save_options}]


{pstd}
Load data from Parquet format

{p 8 17 2}
{cmd:dtparquet use} {it:{help filename}}
[{cmd:,} {it:use_options}]


{synoptset 20 tabbed}{...}
{synopthdr:save_options}
{synoptline}
{syntab:Main}
{synopt:{opt rep:lace}}overwrite existing Parquet file{p_end}
{synopt:{opt vars(varlist)}}subset of variables to save{p_end}
{synoptline}

{synoptset 20 tabbed}{...}
{synopthdr:use_options}
{synoptline}
{syntab:Main}
{synopt:{opt c:lear}}replace data in memory{p_end}
{synopt:{opt vars(varlist)}}subset of variables to keep after loading{p_end}
{synoptline}
{p2colreset}{...}


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtparquet} provides high-performance storage for Stata datasets using the Apache Parquet format. 
Unlike standard Parquet exporters, {cmd:dtparquet} is designed to be a "metadata-aware" bridge, 
ensuring that Stata-specific attributes are preserved when moving data between Stata and other 
tools (or back into Stata).

{pstd}
The command preserves the following metadata:
{p_end}
{phang2}• Variable labels{p_end}
{phang2}• Value labels (mapping of numeric codes to text){p_end}
{phang2}• Stata notes (both dataset-level and variable-level){p_end}
{phang2}• Display formats (e.g., currency, date formats){p_end}
{phang2}• Dataset labels{p_end}

{pstd}
{cmd:dtparquet} requires the {help uv} package manager to be installed on the system. 
It uses an ephemeral Python environment to handle dependencies ({cmd:pandas}, {cmd:pyarrow}) 
automatically, ensuring consistency and isolation.


{marker options}{...}
{title:Options}

{dlgtab:Save Options}

{phang}
{opt replace} permits {cmd:dtparquet save} to overwrite an existing file with the same name.

{phang}
{opt vars(varlist)} specifies a subset of variables to be saved to the Parquet file. 
If not specified, all variables are saved.

{dlgtab:Use Options}

{phang}
{opt clear} specifies that it is okay to replace the data in memory, even if the current 
dataset has changed since it was last saved.

{phang}
{opt vars(varlist)} specifies a subset of variables to be kept in memory after the 
Parquet file is loaded.


{marker remarks}{...}
{title:Remarks}

{pstd}
{ul:Infrastructure}

{pstd}
{cmd:dtparquet} uses a "uv run" architecture. When the command is executed, Stata calls 
Python through the system shell. This approach bypasses the "already initialized" 
limitations of Stata's internal Python engine and ensures that the correct versions 
of {cmd:pandas} and {cmd:pyarrow} are used without requiring any manual virtual 
environment management by the user.

{pstd}
{ul:Metadata Storage}

{pstd}
The Stata metadata is serialized into a JSON blob and stored within the Parquet 
file's schema metadata under the key {cmd:bpom_stata_metadata}. This makes the 
metadata accessible to other Python-based tools while allowing {cmd:dtparquet} 
to perfectly reconstruct the Stata dataset.


{marker examples}{...}
{title:Examples}

{pstd}Setup:{p_end}
{phang2}{cmd:. sysuse auto, clear}{p_end}
{phang2}{cmd:. notes price: "Price in USD"}{p_end}

{pstd}1. Save the entire dataset:{p_end}
{phang2}{cmd:. dtparquet save mydata.parquet, replace}{p_end}

{pstd}2. Save a subset of data with conditions:{p_end}
{phang2}{cmd:. dtparquet save subset.parquet if foreign==1, vars(make price weight) replace}{p_end}

{pstd}3. Load a Parquet file:{p_end}
{phang2}{cmd:. dtparquet use mydata.parquet, clear}{p_end}


{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/hafizarfyanto/dtkit":https://github.com/hafizarfyanto/dtkit}{p_end}


{marker also_see}{...}
{title:Also see}

{psee}
Online: {helpb save}, {helpb use}, {helpb python}, {helpb dtmeta}
{p_end}
