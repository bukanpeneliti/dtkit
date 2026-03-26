# dtkit: Data Toolkit for Stata

[![Stata Package](https://img.shields.io/badge/Stata-ado-blue)](https://github.com/bukanpeneliti/dtkit)
![Version](https://img.shields.io/badge/Version-2.0.6-green)
![Stata 16+](https://img.shields.io/badge/Stata-16%2B-purple)
![GitHub Downloads](https://img.shields.io/github/downloads/bukanpeneliti/dtkit/total)
![GitHub Stars](https://img.shields.io/github/stars/bukanpeneliti/dtkit?style=social)
[![GitHub license](https://img.shields.io/github/license/bukanpeneliti/dtkit.svg)](https://github.com/bukanpeneliti/dtkit/blob/main/LICENSE)

`dtkit` produces structured datasets from your analysis instead of just
printing results to the screen. It uses Stata's frame system for statistics,
frequency tables, and Parquet file access.

## Features

- Creates reusable datasets from analysis results
- Read and write Parquet files directly from Stata
- Exports results to Excel
- Preserves value labels automatically
- Supports all Stata weight types
- Optional gtools integration for faster processing

## Installation

Install from GitHub:

```stata
net install dtkit, from("https://raw.githubusercontent.com/bukanpeneliti/dtkit/main/")
```

First-time Parquet users need to sync the plugin:

```stata
dtkit, update
```

Check plugin status:

```stata
dtkit, pluginstatus
```

## Updating

```stata
dtkit, update
```

Or use the synonym:

```stata
dtkit, upgrade
```

*Note: If you installed before v1.1.0, run `net install dtkit, replace
from(...)` once to enable the update system.*

`dtkit, update` downloads the `dtparquet.dll` plugin from GitHub Releases.
To pin a specific version:

```stata
dtkit, update tag(v2.0.6)
```

## Uninstalling

```stata
ado uninstall dtkit
```

If that doesn't work (multiple installs, corrupted state):

1. Run `ado dir dtkit` and note the index numbers
2. Uninstall by index, highest first:

   ```stata
   ado uninstall [highest_index]
   ado uninstall [next_index]
   ```

## Commands Overview

### dtstat - Descriptive Statistics

Produces datasets with descriptive statistics:

```stata
dtstat price mpg weight
dtstat price mpg, by(foreign)
```

### dtfreq - Frequency Analysis

Produces frequency tables as datasets:

```stata
dtfreq rep78
dtfreq rep78, by(foreign)
```

### dtmeta - Dataset Information

Extracts and saves dataset details:

```stata
dtmeta
dtmeta, save(metadata.xlsx) replace
```

### dtparquet - Parquet Interoperability

Read and write Parquet files:

```stata
dtparquet save "data.parquet", replace
dtparquet use "data.parquet", clear
```

If you get a plugin mismatch error:

```stata
dtkit, update
```

## Performance

Benchmark on 11.16M rows × 20 columns:

| Operation | Stata `.dta` | `dtparquet` |
| :-------- | :----------- | :---------- |
| Full read | 0.91s        | 1.25s       |

Reading and writing Parquet is slower than Stata's native `.dta` format.
That format is highly optimized. The benefit is interoperability: you can
exchange data with Python, R, Spark, and others. For very wide files with
column selection, Parquet can be faster.

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

* Export to Parquet
dtparquet save "auto_data.parquet", replace

* Access results in frames
frame _df: list, noobs clean
frame _dtvars: list varname type format
```

## Compatibility

- Stata 16 or newer
- Windows 11
- `dtparquet` is a plugin binary, no Python or pyarrow needed
- Optional: [gtools](https://github.com/mcaceresb/stata-gtools) for speed boosts

## Support

Report issues or suggest features:  
[GitHub Issues](https://github.com/bukanpeneliti/dtkit/issues)

## Author

Hafiz Arfyanto  
[Email](mailto:bukanpeneliti@gmail.com) | [GitHub](https://github.com/bukanpeneliti)

## Acknowledgments

`dtparquet` draws on ideas and some code structure from
[`stata_parquet_io`](https://github.com/jrothbaum/stata_parquet_io)
(the `pq` command) by Jon Rothbaum at the U.S. Census Bureau. Those
upstream portions are used under the MIT License.

## Citation

If you use `dtkit` in your research:

**Plain Text:**

```text
Hafiz Arfyanto (2026). dtkit: Data Toolkit for Stata. Version 2.0.6.
Retrieved from https://github.com/bukanpeneliti/dtkit
```

**BibTeX:**

```bibtex
@misc{arfyanto2026dtkit,
  author = {Hafiz Arfyanto},
  title = {dtkit: Data Toolkit for Stata},
  version = {2.0.6},
  year = {2026},
  url = {https://github.com/bukanpeneliti/dtkit},
  note = {Stata package for data exploration and analysis}
}
```

For full documentation in Stata:

```Stata
help dtkit
```
