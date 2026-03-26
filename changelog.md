<!-- markdownlint-disable MD013 MD022 MD024 MD032 -->

# Changelog

All notable changes to the dtkit project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
