{smcl}
{* *! version 1.1.0 13Jan2026}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "python" "help python"}{...}
{vieweralsosee "frames" "help frames"}{...}
{viewerjumpto "Syntax" "dtparquet##syntax"}{...}
{viewerjumpto "Description" "dtparquet##description"}{...}
{viewerjumpto "Options" "dtparquet##options"}{...}
{viewerjumpto "Examples" "dtparquet##examples"}{...}
{viewerjumpto "Author" "dtparquet##author"}{...}
{title:Title}

{phang}
{bf:dtparquet} {hline 2} High-performance Parquet I/O using Python/Arrow

{marker syntax}{...}
{title:Syntax}

{pstd}Memory operations{p_end}

{p 8 17 2}
{cmdab:dtparquet save}
{it:filename}
[{cmd:,} {it:save_options}]

{p 8 17 2}
{cmdab:dtparquet use}
[{varlist}]
[{ifin}]
{cmd:using} {it:filename}
[{cmd:,} {it:use_options}]

{pstd}Disk operations (no data loaded into active memory){p_end}

{p 8 17 2}
{cmdab:dtparquet export}
{it:parquetfile}
{cmd:using} {it:dtafile}
[{cmd:,} {it:export_options}]

{p 8 17 2}
{cmdab:dtparquet import}
{it:dtafile}
{cmd:using} {it:parquetfile}
[{cmd:,} {it:import_options}]

{synoptset 24 tabbed}{...}
{synopthdr:save_options}
{synoptline}
{synopt:{opt rep:lace}}overwrite existing file{p_end}
{synopt:{opt nolabel}}suppress writing custom Stata metadata (value labels, etc.){p_end}
{synopt:{opt chunk:size(#)}}batch size for processing; default is 50,000{p_end}
{synoptline}

{synoptset 24 tabbed}{...}
{synopthdr:use_options}
{synoptline}
{synopt:{opt c:lear}}clear data in memory before loading{p_end}
{synopt:{opt nolabel}}suppress reading custom Stata metadata{p_end}
{synopt:{opt chunk:size(#)}}batch size for processing; default is 50,000{p_end}
{synopt:{opt allstring}}import 64-bit integers as strings to preserve precision{p_end}
{synoptline}

{synoptset 24 tabbed}{...}
{synopthdr:export_options}
{synoptline}
{synopt:{opt rep:lace}}overwrite existing file{p_end}
{synopt:{opt nolabel}}suppress writing custom Stata metadata{p_end}
{synopt:{opt chunk:size(#)}}batch size for processing; default is 50,000{p_end}
{synoptline}

{synoptset 24 tabbed}{...}
{synopthdr:import_options}
{synoptline}
{synopt:{opt rep:lace}}overwrite existing file{p_end}
{synopt:{opt nolabel}}suppress reading custom Stata metadata{p_end}
{synopt:{opt chunk:size(#)}}batch size for processing; default is 50,000{p_end}
{synopt:{opt allstring}}import 64-bit integers as strings to preserve precision{p_end}
{synoptline}

{marker description}{...}
{title:Description}

{pstd}
{cmd:dtparquet} provides high-performance reading and writing of Apache Parquet files. It serves as a bridge between 
Stata and the Parquet ecosystem using Python's {cmd:pyarrow} library and Stata's Generic {help sfi:SFI} (Stata Function Interface).

{pstd}
Key features include:

{phang2}• {bf:Metadata Preservation}: Automatically integrates with {help dtmeta} to store and restore value labels, 
variable labels, and notes within the Parquet file footer ({it:dtparquet.dtmeta}).{p_end}
{phang2}• {bf:Memory Efficiency}: Uses a streaming architecture for disk-to-disk operations ({cmd:import} and {cmd:export}), 
allowing processing of datasets larger than available RAM.{p_end}
{phang2}• {bf:Atomic Safety}: Disk operations utilize temporary files ({it:.tmp}) to ensure that the target file is 
only created or updated if the entire operation succeeds.{p_end}
{phang2}• {bf:Type Safety}: Handles complex Stata types including {it:strL} and provides options to handle precision 
limits of 64-bit integers.{p_end}

{marker options}{...}
{title:Options}

{phang}
{opt replace} permits {cmd:dtparquet} to overwrite an existing file.

{phang}
{opt clear} (for {cmd:use}) clears the data in the current frame before loading the Parquet file.

{phang}
{opt nolabel} suppresses the processing of Stata-specific metadata. When saving, value labels and notes are 
not written to the Parquet file. When loading, any existing metadata in the file is ignored. Use this for 
maximum interoperability with other tools (e.g., Spark, Pandas) that do not support Stata metadata.

{phang}
{opt chunksize(#)} specifies the number of observations processed in each batch. The default is 50,000. 
For very wide datasets or low-memory environments, lowering this value can prevent memory exhaustion.

{phang}
{opt allstring} (for {cmd:use} and {cmd:import}) forces 64-bit integers ({it:Int64} and {it:UInt64}) to be imported 
as strings. Stata's {it:double} storage type uses 53 bits for the significand, meaning integers larger than 
9,007,199,254,740,991 (2^53) cannot be represented exactly. Use {opt allstring} to preserve exact digits 
for large ID variables.

{marker examples}{...}
{title:Examples}

{pstd}1. Save the current dataset to Parquet:{p_end}
{phang2}{cmd:. dtparquet save mydata.parquet, replace}{p_end}

{pstd}2. Load specific variables from a Parquet file:{p_end}
{phang2}{cmd:. dtparquet use id price mpg using mydata.parquet, clear}{p_end}

{pstd}3. Import large IDs from a foreign Parquet file as strings:{p_end}
{phang2}{cmd:. dtparquet use using big_ids.parquet, allstring clear}{p_end}

{pstd}4. Export a large .dta file to Parquet without loading it into memory:{p_end}
{phang2}{cmd:. dtparquet export results.parquet using raw_data.dta, replace}{p_end}

{pstd}5. Convert a Parquet file to Stata format on disk:{p_end}
{phang2}{cmd:. dtparquet import final.dta using results.parquet, replace allstring}{p_end}

{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:hafizarfyanto@gmail.com":hafizarfyanto@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/hafizarfyanto/dtkit":https://github.com/hafizarfyanto/dtkit}{p_end}

{pstd}
For questions and suggestions, visit {browse "https://github.com/hafizarfyanto/dtkit/issues":GitHub Issues}.
