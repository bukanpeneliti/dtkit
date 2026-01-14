# Comprehensive Stata .sthlp Style Guide

## File Structure

### Standard Section Order

```text
1. Header block (metadata)
2. Title banner
3. StataNow banner (optional)
4. Syntax
5. Menu
6. Description
7. Links to PDF documentation
8. Options
9. Remarks (if any)
10. Examples
11. Stored results (if applicable)
12. Additional sections (Technical notes, Video, etc.)
```

### Minimal Structure (for overview files)

```text
Header -> Title -> Description -> Links to PDF -> Summary -> Video
```

---

## Header Block

### Standard Header Format

```stata
{smcl}
{* *! version X.Y.Z  DDmonYYYY}{...}
{viewerdialog "command" "dialog dialog_name"}{...}
{vieweralsosee "[D] command" "mansection D entry"}{...}
{vieweralsosee "" "--"}{...}
{vieweralsosee "[D] related" "help related"}{...}
{viewerjumpto "Syntax" "command##syntax"}{...}
{viewerjumpto "Menu" "command##menu"}{...}
{viewerjumpto "Description" "command##description"}{...}
{viewerjumpto "Links to PDF documentation" "command##linkspdf"}{...}
{viewerjumpto "Options" "command##options"}{...}
{viewerjumpto "Examples" "command##examples"}{...}
```

### Version Format

```stata
{* *! version X.Y.Z  DDmonYYYY}{...}
```

- Day: 2 digits (e.g., `04nov2025`)
- Month: 3-letter abbreviation
- Year: 4 digits

### Title Banner

```stata
{p2colset 1 23 25 2}{...}
{p2col:{bf:[D] command} {hline 2}}Brief description{help statanow:+}{p_end}
{p2col:}({mansection D entry:View complete PDF manual entry}){p_end}
{p2colreset}{...}
```

**p2colset values:** `1 12 14 2`, `1 13 15 2`, `1 16 18 2`, `1 17 19 2`, `1 21 18 2`, `1 23 25 2`, `1 25 27 2`

### StataNow Banner (Optional)

```stata
{p 0 2 2}
+This command is part of
{help statanow:StataNow}.
{p_end}
```

---

## Section Markers

### Format

```stata
{marker section_name}{...}
{title:Section Title}
```

**No blank line** between marker and title.

### Common Sections

- `syntax`, `menu`, `description`, `linkspdf`, `options`, `remarks`, `examples`, `video`, `results`
- Sub-sections: `weights`, `overview`, `technote1`, `option_setfredkey`

---

## Syntax Section

### Basic Pattern

```stata
{pstd}
Load a file

{p 8 16 2}
{cmd:command}
[{cmd:using}] {it:{help filename}}
[{cmd:,} {it:options}]
```

### Multiple Variants

Each variant gets its own header:

```stata
{pstd}
Describe contents

{p 8 16 2}
{cmd:import} {cmdab:comm:and}
[{cmd:using}] {it:filename}{cmd:,} {opt desc:ribe}
```

### Indentation Patterns

- `{p 8 12 2}` - Short commands (e.g., `use`)
- `{p 8 16 2}` - Most common
- `{p 8 32 2}` - Longer commands
- `{p 8 17 2}` - collapse
- `{p 12 16 2}` - reshape

### Syntax Components

- Commands: `{cmd:command}`, `{cmdab:ab:br}`, or `{opt command}`
- Options: `{opt option}` or `{opth opt(arg)}`
- Arguments: `{it:argument}` or `{it:{help filename}}`
- Optional: `[` `]`
- Separator: `{cmd:,}`

### Abbreviation Notation

- `{cmdab:exc:el}` -> Minimum abbrev: `exc`, full: `excel`
- `{opt rowr:ange}` -> Minimum abbrev: `rowr`, full: `rowrange`

---

## Description Section

### Writing Style

- **Tense:** Present tense, active voice
- **Length:** 2-4 sentences per paragraph
- **No conversational filler** ("This command allows you to...")

### Description Pattern

```stata
{pstd}
{cmd:command} reads into memory a file format. {cmd:command} can also do X.
{cmd:command} supports Y.
```

### File Extensions

Always format as `{cmd:.ext}`

---

## Options Section

### Format A: Synoptset Table

```stata
{synoptset 26}{...}
{synopthdr :options_name}
{synoptline}
{synopt :{opt clear}}replace data in memory{p_end}
{synopt :{opt rowr:ange}{cmd:(}[{it:start}][{cmd::}{it:end}]{cmd:)}}row range
of data to load{p_end}
{synoptline}
{p 4 6 2}
{cmd:collect} is allowed with {cmd:command}; see {help prefix}.{p_end}
{p 4 6 2}
{opt option()} does not appear in the dialog box.{p_end}
```

### Format B: Description Paragraphs

```stata
{dlgtab:Main}

{phang}
{opt option} specifies that it is okay to replace the data in memory,
even though the current data have not been saved to disk.

{pmore}
    Additional details about option behavior.

{pstd}
The following option is available with {cmd:command} but is not shown
in the dialog box:

{phang}
{opt hidden_option} description.
```

### Required Option Marker

```stata
{p2coldent:* {opth series:list(filename)}}specify series IDs using a file{p_end}
{p 4 6 2}* {opt serieslist()} is required if ...{p_end}
```

---

## Menu Section

### Menu Pattern

```stata
{marker menu}{...}
{title:Menu}

{phang}
{bf:File > Import > Format (*.ext)}
```

### Multiple Commands

```stata
{title:import command}
{phang2}
{bf:File > Import > Format (*.ext)}

{title:export command}
{phang2}
{bf:File > Export > Format (*.ext)}
```

---

## Links to PDF Section

### Links Pattern

```stata
{marker linkspdf}{...}
{title:Links to PDF documentation}

        {mansection D commandQuickstart:Quick start}

        {mansection D commandRemarksandexamples:Remarks and examples}

{pstd}
The above sections are not included in this help file.
```

---

## Examples Section

### Standard Pattern

```stata
{pstd}Setup{p_end}
{phang2}{cmd:. webuse dataset}

{pstd}Description{p_end}
{phang2}{cmd:. command}
```

### With hline Separators

```stata
{hline}
{pstd}Setup{p_end}
{phang2}{cmd:. webuse dataset}

{pstd}Description{p_end}
{phang2}{cmd:. command}
{hline}
```

### Multi-line Commands

```stata
{pstd}Description{p_end}
        {cmd:. command arg1 arg2}
        {cmd:. continue command}
```

---

## Stored Results Section

### Results Pattern

```stata
{marker results}{...}
{title:Stored results}

{pstd}
{cmd:command} stores the following in {cmd:r()}:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Scalars}{p_end}
{synopt :{cmd:r(N)}}number of observations{p_end}
{synopt :{cmd:r(k)}}number of variables{p_end}

{p2col 5 20 24 2: Macros}{p_end}
{synopt :{cmd:r(name)}}description{p_end}
{p2colreset}{...}
```

---

## Formatting Tags

### Paragraph Tags

| Tag          | Description                                        |
| :----------- | :------------------------------------------------- |
| `{pstd}`     | Standard paragraph (`{p 4 4 2}`)                   |
| `{phang}`    | Hanging indent (`{p 4 8 2}`)                       |
| `{phang2}`   | Double hanging indent (`{p 8 12 2}`)               |
| `{pmore}`    | Indented continuation (`{p 8 8 2}`)                |
| `{p 8 16 2}` | Custom indentation (syntax)                        |
| `{p_end}`    | End paragraph                                      |

### Text Formatting

| Tag                | Description                 |
| :----------------- | :-------------------------- |
| `{cmd:text}`       | Command/bold text           |
| `{cmdab:ab:br}`    | Abbreviated command         |
| `{opt option}`     | Option name                 |
| `{opth opt(arg)}`  | Option with argument        |
| `{it:text}`        | Italic/arguments            |
| `{bf:text}`        | Bold                        |
| `{hi:text}`        | Highlighted                 |
| `{c -(}`           | Left brace `{`              |
| `{c -)}`           | Right brace `}`             |

### Structure Tags

| Tag              | Description     |
| :--------------- | :-------------- |
| `{marker name}`  | Section anchor  |
| `{title:Title}`  | Section heading |
| `{hline}`        | Horizontal rule |
| `{break}`        | Line break      |
| `{c \|}`         | Pipe separator  |
| `...`            | End of tag      |

*Note: `{c |}` represents a vertical bar in Stata markup*

### Table Tags

| Tag              | Description         |
| :--------------- | :------------------ |
| `{synoptset N}`  | Set width           |
| `{synopthdr}`    | Table header        |
| `{synoptline}`   | Table line          |
| `{synopt :}`     | Table entry         |
| `{p2colset}`     | 2-column setup      |
| `{p2col :}`      | 2-column item       |

### Box Drawing

```text
{c TLC} {c TRC}  - Top corners
{c BLC} {c BRC}  - Bottom corners
{c |}            - Vertical line
{hline}          - Horizontal line
```

---

## Cross-References

### Internal Links

```stata
{help command}
{help command##section:Link text}
```

### Manual References

```stata
{manhelp command D}
{mansection D entry:Link text}
{mansection D entryQuickstart:Quick start}
```

### External Links

```stata
{browse "https://url.com":Link text}
```

---

## Spacing & Layout

### Blank Lines

- After each `{marker}` and `{title}`
- Between major subsections
- Before `{synoptset}` blocks
- Before each `{phang}` option description

### Indentation

- 2 spaces after periods (traditional)
- 4-8 spaces for continuation lines
- `{phang2}` for example commands

---

## Extension Notes (After Syntax)

```stata
{phang}
If {it:{help filename}} is specified without an extension, {cmd:.ext} is
assumed.  If {it:filename} contains embedded spaces, enclose it in double
quotes.
```

---

## Argument Definitions

```stata
{marker columnlist}{...}
{phang}
{it:columnlist} is a list of column names in the file to be imported.
```

---

## Special Patterns

### Weight Specifications

```stata
{marker weight}{...}
{p 4 6 2}
{opt aweight}s, {opt fweight}s, {opt iweight}s, and {opt pweight}s are
allowed; see {help weight}.
```

### Dialog Box Notes

```stata
{p 4 6 2}{opt option()} does not appear in the dialog box.{p_end}
```

### ASCII Art (reshape example)

```stata
{center:text}
{col N}positioned text
{space N}horizontal spaces
```

---

## File Types

### Full Command Files

All standard sections present.

### Minimal/Overview Files

Only description and links (e.g., `export.sthlp`).

### Multi-Command Files

Separate sections for each command variant.
