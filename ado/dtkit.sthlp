{smcl}
{* *! version 1.1.0  28Jun2025}{...}
{viewerjumpto "Syntax" "dtkit##syntax"}{...}
{viewerjumpto "Description" "dtkit##description"}{...}
{viewerjumpto "Options" "dtkit##options"}{...}
{viewerjumpto "Examples" "dtkit##examples"}{...}
{viewerjumpto "Author" "dtkit##author"}{...}

{title:Title}

{phang}
{bf:dtkit} {hline 2} Data Toolkit package management

{marker syntax}{...}
{title:Syntax}

{p 8 17 2}
{cmdab:dtkit}
[{cmd:,} {it:options}]

{synoptset 20 tabbed}{...}
{synopthdr}
{synoptline}
{syntab:Package Management}
{synopt:{opt upgrade}}upgrade dtkit to latest version{p_end}
{synopt:{opt install_latest}}install latest version of dtkit{p_end}
{synopt:{opt branch(string)}}specify GitHub branch (default: main){p_end}

{syntab:Information}
{synopt:{opt licenses}}display license information{p_end}
{synopt:{opt verbose}}display detailed license information{p_end}

{syntab:Examples and Testing}
{synopt:{opt examples}}run example files for all dtkit commands{p_end}
{synopt:{opt showcase}}same as examples{p_end}
{synopt:{opt test}}run basic functionality tests{p_end}
{synopt:{opt tests(string)}}run specific tests (basic, dtfreq, dtstat, dtmeta){p_end}
{synoptline}
{p2colreset}{...}

{marker description}{...}
{title:Description}

{pstd}
{cmd:dtkit} provides package management functionality for the dtkit suite of data analysis commands.
The dtkit package includes three main commands:

{phang2}
{bf:dtfreq} - Frequency analysis and cross-tabulation

{phang2}
{bf:dtstat} - Descriptive statistics with flexible output options

{phang2}
{bf:dtmeta} - Metadata analysis and variable exploration

{pstd}
When called without options, {cmd:dtkit} displays version information and availability status of all components.

{marker options}{...}
{title:Options}

{dlgtab:Package Management}

{phang}
{opt upgrade} upgrades dtkit to the latest version available on GitHub. This option first uninstalls 
the current version and then installs the latest version from the main branch.

{phang}
{opt install_latest} same as {opt upgrade}.

{phang}
{opt branch(string)} specifies which GitHub branch to use for installation. Default is {bf:main}. 
The {bf:develop} branch may contain experimental features.

{dlgtab:Information}

{phang}
{opt licenses} displays license information for dtkit and its components.

{phang}
{opt verbose} when combined with {opt licenses}, displays the complete license text.

{dlgtab:Examples and Testing}

{phang}
{opt examples} runs the example files for all dtkit commands (dtfreq_examples.do, dtstat_examples.do, 
dtmeta_examples.do). This provides a demonstration of package functionality.

{phang}
{opt showcase} same as {opt examples}.

{phang}
{opt test} runs basic functionality tests to verify that all dtkit commands are properly installed 
and accessible.

{phang}
{opt tests(string)} runs specific test suites. Available options are:
{break}    {bf:basic} - basic functionality tests
{break}    {bf:dtfreq} - dtfreq-specific tests  
{break}    {bf:dtstat} - dtstat-specific tests
{break}    {bf:dtmeta} - dtmeta-specific tests

{marker examples}{...}
{title:Examples}

{phang}{cmd:. dtkit}{p_end}
{phang2}Display version information and component availability{p_end}

{phang}{cmd:. dtkit, upgrade}{p_end}
{phang2}Upgrade dtkit to the latest version{p_end}

{phang}{cmd:. dtkit, examples}{p_end}
{phang2}Run demonstration examples for all dtkit commands{p_end}

{phang}{cmd:. dtkit, licenses}{p_end}
{phang2}Display license information{p_end}

{phang}{cmd:. dtkit, test}{p_end}
{phang2}Run basic functionality tests{p_end}

{phang}{cmd:. dtkit, tests(dtfreq dtstat)}{p_end}
{phang2}Run specific tests for dtfreq and dtstat{p_end}

{marker author}{...}
{title:Author}

{pstd}Hafiz Arfyanto{p_end}
{pstd}Email: {browse "mailto:hafizarfyanto@gmail.com":hafizarfyanto@gmail.com}{p_end}
{pstd}GitHub: {browse "https://github.com/hafizarfyanto/dtkit":https://github.com/hafizarfyanto/dtkit}{p_end}

{pstd}
For questions and suggestions, visit {browse "https://github.com/hafizarfyanto/dtkit/issues":GitHub Issues}.

{marker alsosee}{...}
{title:Also see}

{psee}
Manual:  {bf:[D] dtfreq}, {bf:[D] dtstat}, {bf:[D] dtmeta}

{psee}
Online:  {helpb dtfreq}, {helpb dtstat}, {helpb dtmeta}
{p_end}