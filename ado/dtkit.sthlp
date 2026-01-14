{smcl}
{* *! version 1.1.0  28jun2025}{...}
{vieweralsosee "dtfreq" "help dtfreq"}{...}
{vieweralsosee "dtstat" "help dtstat"}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "dtparquet" "help dtparquet"}{...}
{vieweralsosee "" "--"}{...}
{viewerjumpto "Syntax" "dtkit##syntax"}{...}
{viewerjumpto "Description" "dtkit##description"}{...}
{viewerjumpto "Links to PDF documentation" "dtkit##linkspdf"}{...}
{viewerjumpto "Options" "dtkit##options"}{...}
{viewerjumpto "Examples" "dtkit##examples"}{...}
{viewerjumpto "Author" "dtkit##author"}{...}
{viewerjumpto "Also see" "dtkit##also_see"}{...}
{p2colset 1 12 14 2}{...}
{p2col:{bf:[D] dtkit} {hline 2}}Data Toolkit package management{p_end}
{p2col:}({mansection D dtkit:View complete PDF manual entry}){p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{p 8 16 2}
{cmd:dtkit}
[{cmd:,} {it:options}]

{synoptset 20 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Package Management}
{synopt :{opt upgrade}}upgrade dtkit to latest version{p_end}
{synopt :{opt install_latest}}install latest version of dtkit{p_end}
{synopt :{opt branch(string)}}specify GitHub branch (default: main){p_end}

{syntab:Information}
{synopt :{opt licenses}}display license information{p_end}
{synopt :{opt verbose}}display detailed license information{p_end}

{syntab:Examples and Testing}
{synopt :{opt examples}}run example files for all dtkit commands{p_end}
{synopt :{opt showcase}}same as examples{p_end}
{synopt :{opt test}}run basic functionality tests{p_end}
{synopt :{opt tests(string)}}run specific tests (basic, dtfreq, dtstat, dtmeta, dtparquet){p_end}
{synoptline}


{marker description}{...}
{title:Description}

{pstd}
{cmd:dtkit} manages the suite of data analysis commands.
The package includes four main commands.
It handles installation, updates, and testing across the suite:

{phang2}
{bf:dtfreq} - Frequency analysis and cross-tabulation

{phang2}
{bf:dtstat} - Descriptive statistics with flexible output options

{phang2}
{bf:dtmeta} - Metadata analysis and variable exploration

{phang2}
{bf:dtparquet} - High-performance Parquet I/O using Python/Arrow

{pstd}
{cmd:dtkit} without options displays version information and availability status of all components.
It reports the current installation status and checks for package updates.


{marker linkspdf}{...}
{title:Links to PDF documentation}

{pstd}
No PDF documentation is available for this user-written command.


{marker options}{...}
{title:Options}

{dlgtab:Package Management}

{phang}
{opt upgrade} upgrades dtkit to the latest version available on GitHub.
It uninstalls the current version and installs the latest version from the main branch.

{phang}
{opt install_latest} performs the same action as {opt upgrade}.
It ensures the package matches the latest GitHub release.

{phang}
{opt branch(string)} specifies which GitHub branch to use for installation.  Default is {bf:main}. 
The {bf:develop} branch contains experimental features.

{dlgtab:Information}

{phang}
{opt licenses} displays license information for dtkit and its components.
It lists all applicable open-source licenses.

{phang}
{opt verbose} displays the complete license text when combined with {opt licenses}.
It provides the full legal terms for each component.

{dlgtab:Examples and Testing}

{phang}
{opt examples} runs the example files for all dtkit commands ({cmd:dtfreq_examples.do}, {cmd:dtstat_examples.do}, 
{cmd:dtmeta_examples.do}, {cmd:dtparquet_examples.do}).
These files demonstrate package functionality for {cmd:dtfreq}, {cmd:dtstat}, {cmd:dtmeta}, and {cmd:dtparquet}.

{phang}
{opt showcase} performs the same action as {opt examples}.
It executes all demonstration files in the suite.

{phang}
{opt test} runs basic functionality tests.
It verifies the installation and accessibility of all dtkit commands.

{phang}
{opt tests(string)} runs specific test suites.
Keyword arguments restrict tests to individual commands or functional areas.
Available options are:
{break}    {bf:basic} - basic functionality tests
{break}    {bf:dtfreq} - dtfreq-specific tests  
{break}    {bf:dtstat} - dtstat-specific tests
{break}    {bf:dtmeta} - dtmeta-specific tests
{break}    {bf:dtparquet} - dtparquet-specific tests


{marker examples}{...}
{title:Examples}

{pstd}Display version information and component availability{p_end}
{phang2}{cmd:. dtkit}{p_end}

{pstd}Upgrade dtkit to the latest version{p_end}
{phang2}{cmd:. dtkit, upgrade}{p_end}

{pstd}Run demonstration examples for all dtkit commands{p_end}
{phang2}{cmd:. dtkit, examples}{p_end}

{pstd}Display license information{p_end}
{phang2}{cmd:. dtkit, licenses}{p_end}

{pstd}Run basic functionality tests{p_end}
{phang2}{cmd:. dtkit, test}{p_end}

{pstd}Run specific tests for dtfreq and dtstat{p_end}
{phang2}{cmd:. dtkit, tests(dtfreq dtstat)}{p_end}


{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}{p_end}

{pstd}
Visit {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues} for questions and suggestions.
The issue tracker hosts bug reports and feature requests.


{marker also_see}{...}
{title:Also see}

{psee}
Online: {helpb dtfreq}, {helpb dtstat}, {helpb dtmeta}, {helpb dtparquet}{p_end}
