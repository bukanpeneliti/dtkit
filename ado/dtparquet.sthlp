{smcl}
{* *! version 2.0.7  26mar2026}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "dtkit" "help dtkit"}{...}
{vieweralsosee "[D] use" "help use"}{...}
{vieweralsosee "[D] describe" "help describe"}{...}
{vieweralsosee "[D] save" "help save"}{...}
{vieweralsosee "[D] import" "help import"}{...}
{vieweralsosee "[D] export" "help export"}{...}
{vieweralsosee "frames" "help frames"}{...}
{viewerjumpto "Syntax" "dtparquet##syntax"}{...}
{viewerjumpto "Description" "dtparquet##description"}{...}
{viewerjumpto "Options" "dtparquet##options"}{...}
{viewerjumpto "Remarks" "dtparquet##remarks"}{...}
{viewerjumpto "Examples" "dtparquet##examples"}{...}
{viewerjumpto "Troubleshooting" "dtparquet##troubleshooting"}{...}
{viewerjumpto "Author" "dtparquet##author"}{...}
{p2colset 1 21 23 2}{...}
{p2col:{bf:dtparquet} {hline 2}}High-performance Parquet I/O via native plugin{p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{pstd}
Memory operations

{p 8 16 2}
{cmd:dtparquet} {opt sa:ve} {it:{help filename}} [{cmd:,} {it:save_options}]

{p 8 16 2}
{cmd:dtparquet} {opt u:se} [{varlist}] [{it:if}] [{it:in}] {cmd:using} {it:{help filename}} [{cmd:,} {it:use_options}]

{p 8 16 2}
{cmd:dtparquet} {opt des:cribe} {cmd:using} {it:{help filename}} [{cmd:,} {it:describe_options}]

{pstd}
Disk operations (no data loaded into active memory)

{p 8 16 2}
{cmd:dtparquet} {opt exp:ort} {it:{help filename:parquetfile}} {cmd:using} {it:{help filename:dtafile}} [{cmd:,} {it:export_options}]

{p 8 16 2}
{cmd:dtparquet} {opt imp:ort} {it:{help filename:dtafile}} {cmd:using} {it:{help filename:parquetfile}} [{cmd:,} {it:import_options}]


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtparquet} provides high-performance reading and writing of Apache Parquet
files, offering a fast and efficient alternative to standard Stata I/O for
large-scale datasets.

{pstd}
Key features:

{phang2}
o {bf:Performance}: Leverages a native Rust engine with adaptive batching 
and pushdown filtering for maximum throughput.

{phang2}
o {bf:Metadata Preservation}: Automatically stores and restores value labels, 
variable labels, and notes using {help dtmeta}.

{phang2}
o {bf:Frame Isolation}: Disk-to-disk operations ({cmd:import} and {cmd:export}) 
preserve the active dataset by running in background frames.

{phang2}
o {bf:Type Safety}: Full support for complex Stata types, including {it:strL} 
and precision-sensitive 64-bit integers.


{marker options}{...}
{title:Options}

{pstd}
Options are presented under the following headings:

{phang2}
{help dtparquet##save_options:Options for dtparquet save}{p_end}
{phang2}
{help dtparquet##use_options:Options for dtparquet use}{p_end}
{phang2}
{help dtparquet##describe_options:Options for dtparquet describe}{p_end}
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
{synopt :{opt nol:abel}}suppress writing custom Stata metadata (labels, notes){p_end}
{synopt :{opt ch:unksize(#)}}batch size for processing; {cmd:0} uses adaptive sizing{p_end}
{synopt :{opt com:press(codec)}}compression codec; default {cmd:fast} (lz4). 
Presets: {cmd:fast}, {cmd:balanced} (zstd), {cmd:archive} (brotli). 
Also allowed: {cmd:lz4}, {cmd:snappy}, {cmd:gzip}, {cmd:brotli}, {cmd:zstd}, {cmd:lzo}, {cmd:uncompressed}.{p_end}
{synopt :{opt part:itionby(varlist)}}write partitioned Parquet output by variables{p_end}
{synopt :{opt timer(mode)}}timer mode: {cmd:stata}, {cmd:plugin}, {cmd:all}, or {cmd:off} (default){p_end}
{synoptline}

{marker describe_options}{...}
{dlgtab:Options for dtparquet describe}

{synoptset 26 tabbed}{...}
{synopthdr :describe_options}
{synoptline}
{synopt :{opt quietly}}suppress printed output and keep returned results{p_end}
{synopt :{opt short}}show the file summary without the variable table{p_end}
{synopt :{opt simple}}list variable names only{p_end}
{synopt :{opt fullnames}}show full variable names instead of abbreviated names{p_end}
{synopt :{opt numbers}}add variable numbers to the output table{p_end}
{synopt :{opt detailed}}compute string lengths and show them in the Stata type column{p_end}
{synopt :{opt replace}}clear memory and load the schema description as a dataset{p_end}
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
{synopt :{opt cat:mode(mode)}}handling for foreign categorical/enum columns:
{cmd:encode} (default), {cmd:raw}, or {cmd:both}.{p_end}
{synopt :{opt timer(mode)}}timer mode: {cmd:stata}, {cmd:plugin}, {cmd:all}, or {cmd:off} (default){p_end}
{synoptline}

{marker export_options}{...}
{dlgtab:Options for dtparquet export}

{synoptset 26 tabbed}{...}
{synopthdr :export_options}
{synoptline}
{synopt :{opt re:place}}overwrite existing file{p_end}
{synopt :{opt nol:abel}}suppress writing custom Stata metadata{p_end}
{synopt :{opt timer(mode)}}timer mode: {cmd:stata}, {cmd:plugin}, {cmd:all}, or {cmd:off} (default){p_end}
{synoptline}

{marker import_options}{...}
{dlgtab:Options for dtparquet import}

{synoptset 26 tabbed}{...}
{synopthdr :import_options}
{synoptline}
{synopt :{opt re:place}}overwrite existing file{p_end}
{synopt :{opt nol:abel}}suppress reading custom Stata metadata{p_end}
{synopt :{opt all:string}}import 64-bit integers as strings to preserve precision{p_end}
{synopt :{opt timer(mode)}}timer mode: {cmd:stata}, {cmd:plugin}, {cmd:all}, or {cmd:off} (default){p_end}
{synoptline}

{pstd}
Use {opt timer(mode)} to control timer output in one place:
{break}{cmd:stata}: show Stata elapsed summary, disable Rust telemetry macros
{break}{cmd:plugin}: enable plugin telemetry macros, suppress Stata elapsed summary
{break}{cmd:all}: enable both
{break}{cmd:off}: disable both (default)


{marker remarks}{...}
{title:Remarks}

{pstd}
{bf:Precision of 64-bit Integers}
{break}Stata's {cmd:double} type has 53 bits of mantissa, meaning it can only 
represent integers exactly up to 9,007,199,254,740,992 (2^53). Parquet files 
from other systems (like Spark or Python) often contain 64-bit integers that 
exceed this limit. By default, {cmd:dtparquet} will cast these to {cmd:double}, 
potentially losing precision. Use the {opt allstring} option to import these 
columns as Stata {it:strL} variables to preserve the exact values.

{pstd}
{bf:Performance and Pushdown Filtering}
{break}{cmd:dtparquet} is optimized for speed. When using {cmd:dtparquet use}, 
Stata's {help if} and {help in} qualifiers are "pushed down" to the native 
plugin. This allows the plugin to filter rows during the scan phase, 
transferring only the required data into Stata memory. This is significantly 
faster than loading the entire file and filtering in Stata.

{pstd}
{bf:Categorical Data}
{break}Foreign Parquet files often contain "Categorical" or "Enum" types. 
{cmd:dtparquet} can handle these in three ways via {opt catmode()}:
{break}{cmd:encode}: Converts categories to Stata value labels (default).
{break}{cmd:raw}: Imports the underlying string categories as string variables.
{break}{cmd:both}: Keeps the encoded numeric variable and creates a companion 
string variable.

{pstd}
{bf:Resource Control}
{break}By default, the plugin uses all available CPU threads for parallel I/O. 
You can limit this by setting the {cmd:DTPARQUET_THREADS} environment variable 
before running the command.


{marker examples}{...}
{title:Examples}

{pstd}Save the current dataset to Parquet (abbreviated){p_end}
{phang2}{cmd:. dtparquet sa mydata.parquet, re}{p_end}

{pstd}Save partitioned output by selected variables{p_end}
{phang2}{cmd:. dtparquet sa sales, partitionby(year region)}{p_end}

{pstd}Load specific variables from a Parquet file (abbreviated){p_end}
{phang2}{cmd:. dtparquet u id price mpg using mydata.parquet, c}{p_end}

{pstd}Import large IDs from a foreign Parquet file as strings{p_end}
{phang2}{cmd:. dtparquet use using big_ids.parquet, allstring clear}{p_end}

{pstd}Inspect a Parquet file without loading the data{p_end}
{phang2}{cmd:. dtparquet describe using big_ids.parquet, numbers detailed}{p_end}

{pstd}Export a .dta file to Parquet while preserving the active frame{p_end}
{phang2}{cmd:. dtparquet export results.parquet using raw_data.dta, replace}{p_end}

{pstd}Convert a Parquet file to Stata format on disk{p_end}
{phang2}{cmd:. dtparquet import final.dta using results.parquet, replace allstring}{p_end}


{marker troubleshooting}{...}
{title:Troubleshooting}

{phang}
If you see a plugin mismatch or missing-binary error, run {cmd:dtkit, update}.

{phang}
After a fresh {cmd:net install dtkit}, run {cmd:dtkit, update} once to fetch
the plugin binary.

{phang}
If your network blocks GitHub release assets, manually download
{cmd:dtparquet.dll} from
{browse "https://github.com/bukanpeneliti/dtkit/releases":GitHub Releases}
and place it in your dtkit ado directory (typically {cmd:`c(sysdir_plus)'d/}).

{phang}
Use {cmd:dtkit, pluginstatus} to inspect ado path, plugin path/presence, and
loaded plugin version.


{marker author}{...}
{title:Author}

{pstd}
Hafiz Arfyanto{break}
Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{break}
GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}

{pstd}
For questions and suggestions, visit {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues}.

{marker also_see}{...}
{title:Also see}

{psee}
Manual: {manlink D use}, {manlink D describe}, {manlink D save}, {manlink D import}, {manlink D export}

{psee}
Online: {helpb use}, {helpb describe}, {helpb save}, {helpb import}, {helpb export}, {helpb dtmeta}, {helpb frames}
