{smcl}
{* *! version 2.0.7  26mar2026}{...}
{vieweralsosee "dtfreq" "help dtfreq"}{...}
{vieweralsosee "dtstat" "help dtstat"}{...}
{vieweralsosee "dtmeta" "help dtmeta"}{...}
{vieweralsosee "dtparquet" "help dtparquet"}{...}
{vieweralsosee "" "--"}{...}
{viewerjumpto "Syntax" "dtkit##syntax"}{...}
{viewerjumpto "Description" "dtkit##description"}{...}
{viewerjumpto "Options" "dtkit##options"}{...}
{viewerjumpto "Examples" "dtkit##examples"}{...}
{viewerjumpto "Author" "dtkit##author"}{...}
{viewerjumpto "Also see" "dtkit##also_see"}{...}
{p2colset 1 12 14 2}{...}
{p2col:{bf:dtkit} {hline 2}}Data Toolkit package management{p_end}
{p2colreset}{...}


{marker syntax}{...}
{title:Syntax}

{p 8 16 2}
{cmd:dtkit} [{cmd:,} {it:options}]

{synoptset 20 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Package Management}
{synopt :{opt update}}update dtkit to latest version{p_end}
{synopt :{opt upgrade}}same as update{p_end}
{synopt :{opt branch(string)}}specify GitHub branch (default: main){p_end}
{synopt :{opt tag(string)}}download dtparquet plugin from a specific GitHub release tag{p_end}
{synopt :{opt pluginstatus}}show dtparquet plugin file and version status{p_end}

{syntab:Information}
{synopt :{opt licenses}}display license information{p_end}
{synopt :{opt verbose}}display detailed license information{p_end}

{syntab:Examples and Testing}
{synopt :{opt examples}}run example files for all dtkit commands{p_end}
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
{bf:dtparquet} - High-performance Parquet I/O using native plugin runtime

{pstd}
{cmd:dtkit} without options displays version information and availability status of all components.
It reports the current installation status and checks for package updates.


{marker options}{...}
{title:Options}

{dlgtab:Package Management}

{phang}
{opt update} updates dtkit to the latest version available on GitHub.
It uninstalls the current version and installs the latest version from the main branch.

{phang}
{opt upgrade} performs the same action as {opt update}.
It ensures the package matches the latest GitHub release.

{phang}
{opt branch(string)} specifies which GitHub branch to use for installation.  Default is {bf:main}. 
The {bf:develop} branch contains experimental features.

{phang}
{opt tag(string)} selects a specific GitHub release tag for downloading the
{cmd:dtparquet.dll} binary during {opt update}/{opt upgrade}.  If omitted,
the latest release asset is used.

{phang}
{opt pluginstatus} prints the detected {cmd:dtparquet} plugin location,
whether {cmd:dtparquet.dll} is present, and the loaded plugin version.

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
{opt test} runs basic functionality tests.
It verifies the installation and accessibility of all dtkit commands.

{phang}
{opt tests(string)} runs specific test suites.
Keyword arguments restrict tests to individual commands or functional areas.
Available options are:

{phang2}
{bf:basic} - basic functionality tests

{phang2}
{bf:dtfreq} - dtfreq-specific tests

{phang2}
{bf:dtstat} - dtstat-specific tests

{phang2}
{bf:dtmeta} - dtmeta-specific tests

{phang2}
{bf:dtparquet} - dtparquet-specific tests


{marker examples}{...}
{title:Examples}

{pstd}Display version information and component availability{p_end}
{phang2}{cmd:. dtkit}{p_end}

{pstd}Upgrade dtkit to the latest version{p_end}
{phang2}{cmd:. dtkit, upgrade}{p_end}

{pstd}Upgrade and force a specific release-tagged plugin binary{p_end}
{phang2}{cmd:. dtkit, update tag(v2.0.7)}{p_end}

{pstd}Check dtparquet plugin install/version status{p_end}
{phang2}{cmd:. dtkit, pluginstatus}{p_end}

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

{pstd}
Hafiz Arfyanto{break}
Email: {browse "mailto:bukanpeneliti@gmail.com":bukanpeneliti@gmail.com}{break}
GitHub: {browse "https://github.com/bukanpeneliti/dtkit":https://github.com/bukanpeneliti/dtkit}

{pstd}
Visit {browse "https://github.com/bukanpeneliti/dtkit/issues":GitHub Issues} for questions and suggestions.


{marker also_see}{...}
{title:Also see}

{psee}
Online: {helpb dtfreq}, {helpb dtstat}, {helpb dtmeta}, {helpb dtparquet}{p_end}
