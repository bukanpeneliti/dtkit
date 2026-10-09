<!-- markdownlint-disable MD013 MD022 MD024 MD032 -->

# Changelog

All notable changes to the dtkit project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Package Release [v2.1.0] - 2026-10-09

- Feature release for survey-weighted estimation in `dtfreq` and `dtstat`, and
  a polars upgrade with a plugin unload-crash fix in `dtparquet`.
- Component versions:
  - **dtkit: v2.1.0 (Updated)**
  - **dtfreq: v1.1.0 (Updated)**
  - **dtstat: v1.1.0 (Updated)**
  - dtmeta: v1.0.2 (Unchanged)
  - **dtparquet: v2.0.10 (Updated)**

### Added

- **dtfreq v1.1.0**:
  - Survey tabulation: with `[pw=]` and an active `svyset` design, `dtfreq`
    estimates through `svy: tabulate` semantics and reports weighted
    frequencies and proportions alongside unweighted counts, with linearized
    standard errors and t-based Wald confidence intervals controlled by the
    new `level()` option.
  - `subpop(varname | if)` option for domain estimation that retains the full
    survey design for variance, including per-level combination with `by()`.
  - Bare `[pw]` inherits the sampling weight from the active `svyset`.
  - Guards mirroring `svy` behavior: data not svyset (r119), design without
    sampling weights (r119), weight mismatch against the design (r198),
    `subpop()` without pweights (r198), empty subpopulation domain (r461).
  - New regression suite `dtfreq_test3.do` with 13 cases benchmarked against
    `svy: tabulate`, including a designed-in singleton stratum, zero weights,
    and missing weights.

- **dtstat v1.1.0**:
  - Design-based statistics: with `[pw=]` and an active `svyset` design,
    `dtstat` delegates to `svy: mean`, `svy: total`, and `svy: ratio` and
    reports `estimate`, `se`, `ci_l`, `ci_u`, `df`, `n_unw`, and `n_w` for
    each variable and statistic.
  - `subpop(varname | if)` domain estimation; `by()` groups are estimated as
    subpopulations so the full design drives every group's variance, with the
    overall row estimated on the whole domain.
  - Explicit `svy` option for design-based estimation; omitting `svy` defaults
    to standard `collapse` behavior even when a survey design is active.
  - Ratio statistics via `num/den` varlist terms in svy mode.
  - New regression suite `dtstat_test3.do` with 10 cases benchmarked against
    `svy: mean`, `svy: total`, and `svy: ratio`, including singleton strata,
    empty subpopulation domains, and by-group subpopulation estimation.

### Changed

- **dtparquet v2.0.10**:
  - Upgraded polars from 0.53.0 to 0.55.2 (enabling its `streaming` feature
    and adapting to the 0.55 API renames) and `polars-readstat-rs` from
    0.20.1 to 0.24.2.

### Fixed

- **dtparquet v2.0.10**:
  - Pinned the plugin image in memory at load time and stopped dropping and
    redefining the plugin program on every execution. Dropping the plugin
    while its rayon worker threads were still alive unloaded the DLL under
    them and crashed Stata (0xC0000005); the image is now pinned for the
    process lifetime and a re-`plugin using` reuses it.
  - A failed plugin load now reports the DLL path and raises r601 instead of
    a bare program-definition error.

## Package Release [v2.0.10] - 2026-09-10

- Patch release for the `dtmeta` line-break value label fix.
- Component versions:
  - **dtkit: v2.0.10 (Updated)**
  - **dtmeta: v1.0.2 (Updated)**
  - dtfreq: v1.0.2 (Unchanged)
  - dtstat: v1.0.2 (Unchanged)
  - dtparquet: v2.0.9 (Unchanged)

### Fixed

- **dtmeta v1.0.2**:
  - Read value labels with Mata (`st_vldir`/`st_vlload`) instead of
    `uselabel`, which round-trips label text through a temp do-file and
    fails with `in not found` r(111) when label text contains a line feed
    or carriage return.
  - Guarded the internal `labelbook` call against the same r(111) failure
    since `labelbook` uses `uselabel` internally.
  - Added regression coverage: `dtmeta` Test 12 (LF/CR label preservation)
    and `dtparquet` Test 11 (LF label save/use roundtrip).

## Package Release [v2.0.9] - 2026-08-06

- Patch release for quiet `strL` loading and reproducible release builds.
- Component versions:
  - **dtkit: v2.0.9 (Updated)**
  - **dtparquet: v2.0.9 (Updated)**

### Changed

- **Release workflow**:
  - Pinned the Rust toolchain to 1.93.0 to avoid an `ethnum` 1.5.2 build
    failure under the floating stable toolchain.

### Fixed

- **dtparquet v2.0.9**:
  - Suppressed the internal merge table and no-observations-deleted message
    emitted while loading `strL` columns.

## Package Release [v2.0.8] - 2026-08-06

- Patch release for reliable `strL` loading and local Parquet inspection.
- Component versions:
  - **dtkit: v2.0.8 (Updated)**
  - **dtparquet: v2.0.8 (Updated)**

### Fixed

- **dtparquet v2.0.8**:
  - Loaded `strL` columns through a temporary DTA merge because Stata's plugin
    API cannot write `strL` values directly.
  - Preserved `strL` storage for short values and supported selections that
    contain only `strL` columns.
  - Read local Parquet schemas and projected string columns eagerly to avoid
    the Polars lazy Tokio path during `describe, detailed`.
  - Mapped overlength Parquet column names to unique, valid Stata names while
    retaining the original names for Parquet column selection.
  - Initialized variable-note state for foreign Parquet files so
    `describe, detailed` renders files without embedded `dtmeta` metadata.

## Package Release [v2.0.7] - 2026-03-26

- Patch release to align `dtparquet describe` output with Stata conventions.
- Component versions:
  - **dtkit: v2.0.7 (Updated)**
  - **dtparquet: v2.0.7 (Updated)**

### Fixed

- **dtparquet v2.0.7**:
  - Added default display formats for foreign Parquet columns.
  - Displayed variable-note markers and metadata-backed formats, value labels,
    and variable labels in the aligned schema table.
  - Updated `describe, replace` to use default formats when embedded `dtmeta`
    formats are absent.

## Package Release [v2.0.6] - 2026-03-26

- Patch release to align package/plugin version metadata and complete
  Stata-style `dtparquet describe` output fields.
- Component versions:
  - **dtkit: v2.0.6 (Updated)**
  - **dtparquet: v2.0.6 (Updated)**

### Changed

- **dtparquet v2.0.6**:
  - Updated `describe` table columns to `Variable name`, `Storage type`,
    `Display format`, `Value label`, and `Variable label`.
  - Updated `describe, replace` to populate `format`, `vallab`, and `varlab`
    directly from embedded `dtmeta` metadata.
  - Standardized package and plugin version metadata to `2.0.6` across
    release files.

## Package Release [v2.0.5] - 2026-03-25

- Patch release for `dtparquet describe` output and timing checks.
- Component versions:
  - **dtkit: v2.0.5 (Updated)**
  - **dtparquet: v2.0.5 (Updated)**

### Fixed

- **dtparquet v2.0.5**:
  - Restyled `dtparquet describe` to use aligned, native-style output.
  - Added dataset label, timestamp, and note header details when `dtmeta`
    metadata is present.
  - Skipped metadata loading for foreign Parquet files without the
    `dtparquet.dtmeta` key.
  - Added regression coverage for the large foreign-file describe path.

## Package Release [v2.0.4] - 2026-03-25

- Patch release for `dtparquet describe` support and help coverage.
- Component versions:
  - **dtkit: v2.0.4 (Updated)**
  - **dtparquet: v2.0.4 (Updated)**

### Fixed

- **dtparquet v2.0.4**:
  - Routed `dtparquet describe` through the plugin's `describe` subcommand.
  - Added help-file syntax and option docs for `dtparquet describe`.
  - Added regression coverage for `fullnames`, `numbers`, and `replace`.

## Package Release [v2.0.3] - 2026-03-24

- Patch release for dtparquet metadata restoration.
- Component versions:
  - **dtkit: v2.0.3 (Updated)**
  - **dtparquet: v2.0.3 (Updated)**

### Fixed

- **dtparquet v2.0.3**:
  - Restored value labels even when the source dataset has no dataset label.
  - Separated value-label restoration from `label data` restoration.
  - Added a regression test for roundtripping value labels without `label
    data`.

## Package Release [v2.0.2] - 2026-03-13

- **Patch release** for dtparquet `use` performance and benchmark reliability.
- Component versions:
  - **dtkit: v2.0.2 (Updated)**
  - **dtparquet: v2.0.2 (Updated)**

### Fixed

- **dtparquet v2.0.2**:
  - Reduced numeric sink overhead in typed `use` paths with contiguous no-null
    fast paths.
  - Preserved `if_filter_mode` state macro under `timer(off)` to keep test
    behavior consistent.
  - Consolidated benchmark and test coverage updates used for release
    validation.

## Package Release [v2.0.1] - 2026-03-11

- **Critical performance optimization** for the Rust plugin.
- Component versions:
  - **dtkit: v2.0.1 (Updated)**
  - **dtparquet: v2.0.1 (Updated)**

### Fixed
- **dtparquet v2.0.1**:
  - Eliminated O(N²) iterator scaling in parallel save operations (Replaced .skip().take() with O(1) slices).
  - Optimized Stata-to-Rust missingness check using pure Rust bitwise comparisons (Removed 1M+ FFI calls per column).
  - Improved string transfer performance using `StringChunkedBuilder` to reduce individual allocations.
  - Performance: ~2.3x faster saves and ~2.1x faster reads compared to v2.0.0 baseline.

## Package Release [v2.0.0] - 2026-03-02

- **Major architectural update** with Rust plugin migration to Polars 0.53, extensive performance optimizations, and stability hardening.
- Component versions:
  - **dtkit: v2.0.0 (Updated)**
  - **dtparquet: v2.0.0 (Updated)**
  - dtfreq: v1.0.2 (Unchanged)
  - dtstat: v1.0.2 (Unchanged)
  - dtmeta: v1.0.1 (Unchanged)

### Changed

- **dtparquet v2.0.0**: Major Rust plugin overhaul
  - **Polars 0.53 Migration**: Upgraded from Polars 0.52 → 0.53 with full backward compatibility
  - **Performance Optimization (Phase 12)**:
    - Bounds validation hoisting for reduced overhead
    - Batched counter publication replacing per-cell atomic increments
    - Reusable string buffers for reduced CString allocations
    - Conditional parallelization based on workload justification
    - Write-stage timing instrumentation (separates collect time from parquet serialization)
  - **Crash Hardening**:
    - Changed `panic="unwind"` → `panic="abort"` for FFI safety
    - Added `AssertUnwindSafe` wrapper for segfault prevention
    - FFI entrypoint argument validation
    - Thread-pool graceful degradation
  - **Code Quality**: ~1,000 lines reduced through aggressive refactoring
  - **Enhanced Testing**:
    - New stress test: `dtparquet_test8.do` (1,000 setup_check iterations, 200 save/use roundtrips)
    - Comprehensive benchmark suite: `dtparquet_vs_pq.do` (compares vs Stata's `pq` command)
    - Timer instrumentation across all test cases

## Package Release [v1.1.0] - 2026-01-14

- **Major feature update** introducing Parquet support and centralized package management.
- Transitioned project identity and contact info to `bukanpeneliti`.
- Component versions:
  - **dtkit: v1.1.0 (Updated)**
  - **dtparquet: v1.0.0 (New)**
  - dtfreq: v1.0.2 (Doc update)
  - dtstat: v1.0.2 (Doc update)
  - dtmeta: v1.0.1 (Doc update)

### Added
- **dtparquet v1.0.0**: New module for high-performance Parquet file interoperability.
  - Native Python/pyarrow integration for speed and reliability.
  - Supports `save`, `use`, `import`, and `export` subcommands.
  - Preserves Stata metadata (labels, notes) within Parquet schema.
- **dtkit management**: Added `update`, `upgrade`, `test`, and `showcase` options.
- **Cleanup Utility**: Added `cleanup_test_logs.do` for automated test artifact management.

### Changed
- **Project Identity**: Updated all contact info to `bukanpeneliti@gmail.com` and GitHub username to `bukanpeneliti`.
- **Documentation Style**: Refactored all `.sthlp` files for strict compliance with `guide_sthlp.md`.
  - Converted all text to active voice and present tense.
  - Standardized SMCL layout and title banners.
- **UI Refinement**: Removed emojis and standardized non-ASCII symbols across all commands and test outputs.

### Improved
- **Test Infrastructure**: Enhanced `run_all_tests.do` with better progress tracking and summary reporting.
- **Package Metadata**: Synchronized `dtkit.pkg` and `stata.toc` with the new component structure.

## Package Release [dtkit-v1.0.1] - 2025-06-03

- This update includes an important bug fix for the `dtfreq` and `dtstat` commands.
- Component versions included in this release:
  - **dtfreq: v1.0.1 (Updated)**
  - **dtstat: v1.0.1 (Updated)**
  - dtmeta: v1.0.0 (Unchanged)

### Fixed

- **dtfreq v1.0.1**:
  - Fixed an issue where the "Total" row in tables created with the `cross()` option sometimes showed incorrect calculations for proportions and percentages. Totals are now accurate.
- **dtstat v1.0.1**:
  - Fixed unused internal subroutine for sample marking.
  
## Package Release [dtkit-v1.0.0] - 2025-06-02

- **First official release** of the `dtkit` tools for Stata.
- All tools (`dtfreq`, `dtstat`, `dtmeta`) have been fully updated for better performance and easier use.
- Thoroughly tested for reliability.

### Added

New tools included in this release:

- **dtstat v1.0.0**: For descriptive statistics.
  - Choose from many stats (count, mean, median, sd, min, max, sum, iqr, percentiles).
  - Get stats for groups, with automatic totals.
  - Uses your chosen number format, instead of auto-formatting.
  - Export to Excel with multiple sheets and options.
  - Faster if you have `gtools` installed (optional).

- **dtfreq v1.0.0**: For frequency tables and cross-tabulations.
  - Create one-way and two-way tables easily.
  - Shows row, column, and cell proportions/percentages.
  - Helps organize data for variables with two categories (e.g., yes/no).
  - Automatically adds totals to cross-tabulations.
  - Keeps your value labels and formats numbers smartly.
  - Export to Excel with options to customize sheets.

- **dtmeta v1.0.0**: To get information about your dataset.
  - Extracts details about variables, value labels, and notes into new, organized dataset (frames).
  - Also provides general information about your dataset.
  - Export all this information to Excel, with multiple sheets.
  - Includes commands to easily view these new tables.
  - Works well even if your dataset doesn't have many notes or labels.

### Improved

General improvements for all tools:

- **File Saving**: More reliable when saving files.
- **Excel Export**: Consistent Excel export options across all tools.
- **Help Files**: Updated help files with more examples.
- **Reliability**: Increased reliability from extensive testing.
- **Table Handling**: Better management of the tables (frames) created by the tools.
- **Stata Compatibility**: Works with Stata 16 and newer.
- **Weight Support**: Supports all standard Stata weight types.
- **Error Messages**: More helpful error messages.
- **Documentation**: README file updated with clear installation and usage steps. Citation information provided for academic use.

---

## Version Tag Strategy

- **dtkit-vX.Y.Z**: Overall package releases
- **dtstat-vX.Y.Z**: dtstat module-specific releases  
- **dtfreq-vX.Y.Z**: dtfreq module-specific releases
- **dtmeta-vX.Y.Z**: dtmeta module-specific releases

## Previous Versions

### Pre-v1.0.0

- Development versions with inconsistent functionality
- Mixed version numbers across modules
- Limited documentation and test coverage
