# dtkit: Data Toolkit for Stata

[![Stata Package](https://img.shields.io/badge/Stata-ado-blue)](https://github.com/bukanpeneliti/dtkit)
![Version](https://img.shields.io/badge/Version-1.0.1-green)
![Stata 16+](https://img.shields.io/badge/Stata-16%2B-purple)
![GitHub Downloads](https://img.shields.io/github/downloads/bukanpeneliti/dtkit/total)
![GitHub Stars](https://img.shields.io/github/stars/bukanpeneliti/dtkit?style=social)
[![GitHub license](https://img.shields.io/github/license/bukanpeneliti/dtkit.svg)](https://github.com/bukanpeneliti/dtkit/blob/main/LICENSE)

`dtkit` is a Stata package that transforms data exploration by creating **structured datasets** instead of display-only results. It uses Stata's frame system to deliver improved statistics, frequency analysis, and dataset information.

## Features

- **Creates reusable datasets** from analysis results
- **Exports directly to Excel**
- Preserves value labels automatically
- Supports all Stata weight types
- Optional faster processing with gtools

## Installation

Install `dtkit` directly from GitHub using Stata's `net install` command:

```stata
net install dtkit, from("https://raw.githubusercontent.com/bukanpeneliti/dtkit/main/")
```

## Updating to Latest Version
To ensure you have the most recent features and bug fixes:

```stata
net install dtkit, replace from("https://raw.githubusercontent.com/bukanpeneliti/dtkit/main/")
```

## Uninstalling
If you need to remove the package:

```stata
ado uninstall dtkit
```

### Alternative Uninstall Method
If the standard uninstall method doesn't work (e.g., if dtkit was installed multiple times), we can follow these steps:

1. Run: `ado dir dtkit` in Stata command window
2. Note all index numbers shown for dtkit installations
3. Uninstall packages using index numbers in descending order:
   ```stata
   ado uninstall [highest_index]
   ado uninstall [next_index]
   ```

## Commands Overview

### `dtstat` - Descriptive Statistics
Creates datasets with descriptive statistics

```stata
dtstat price mpg weight
dtstat price mpg, by(foreign)
```

### `dtfreq` - Frequency Analysis
Generates frequency tables as datasets

```stata
dtfreq rep78
dtfreq rep78, by(foreign)
```

### `dtmeta` - Dataset Information
Extracts details about your dataset

```stata
dtmeta
dtmeta, save(metadata.xlsx) replace
```

## Practical Workflow

```stata
* Load data
sysuse auto, clear

* Extract dataset information
dtmeta

* Analyze numerical variables
dtstat price mpg weight, by(foreign)

* Examine categorical distributions
dtfreq rep78, by(foreign)

* Access results in frames
frame _df: list, noobs clean
frame _dtvars: list varname type format
```

## Compatibility
- Requires Stata 16 or newer
- Windows 11 compatible
- Optional: [`gtools`](https://github.com/mcaceresb/stata-gtools) for speed boost

## Support
Report issues or suggest improvements:  
[GitHub Issues](https://github.com/bukanpeneliti/dtkit/issues)

## Author
Hafiz Arfyanto  
[Email](mailto:bukanpeneliti@gmail.com) | [GitHub](https://github.com/bukanpeneliti)

## Citation

If you use `dtkit` in your research, please cite:

**Plain Text:**
```
Hafiz Arfyanto (2025). dtkit: Data Toolkit for Stata. Version 1.0.0.
Retrieved from https://github.com/bukanpeneliti/dtkit
```

**BibTeX Entry:**
```bibtex
@misc{arfyanto2025dtkit,
  author = {Hafiz Arfyanto},
  title = {dtkit: Data Toolkit for Stata},
  version = {1.0.0},
  year = {2025},
  url = {https://github.com/bukanpeneliti/dtkit},
  note = {Stata package for data exploration and analysis}
}
```

*For detailed documentation, see the official help file in Stata*
```Stata
help dtkit
```
