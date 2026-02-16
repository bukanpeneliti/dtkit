# Phase 7: dtparquet Modularization Implementation Plan

## Overview

This document outlines the implementation plan for Phase 7 of dtparquet development: modularizing the Rust plugin to improve code comprehension, testability, and maintainability.

## Context

This phase builds upon completed work:
- **Phase 1**: Initial dtparquet implementation (Python bridge)
- **Phase 2** (HANDOFF.md): Rust plugin rewrite with Polars 0.52.0 upgrade, including:
  - Slice 1: SQL translation characterization coverage
  - Slice 2: Parser architecture for `sql_from_if.rs`
  - Slice 3: Utilities thread precedence refactor
  - Slice 4: Typed command structs in `lib.rs`
  - Slice 5: Compatibility shim cleanup

Phase 7 focuses on extracting modules to reduce file size and improve maintainability while preserving all runtime contracts.

## Validation Workflow

Based on HANDOFF.md, all changes must follow this workflow:

### 1. Build the Plugin
```bash
cd plugin && cargo build --release
```

### 2. Lock-Safe DLL Promotion
```
ado/ancillary_files/dtparquet.new.dll → ado/ancillary_files/dtparquet.dll
```
- Use `dtparquet.new.dll` only when `dtparquet.dll` is locked

### 3. Run Validation Tests
Run **one-by-one** in batch mode:
```bash
cd "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/log" 
"C:/Program Files/StataNow19/StataMP-64.exe" -e do "D:/OneDrive/MyWork/00personal/stata/dtkit/ado/ancillary_files/test/dtparquet/dtparquet_testN.do"
```
Replace `N` with 1-7 for each test file.

### 4. Verify Policy Invariants
After each slice:
- Metadata restore remains in-parquet-only (`dtparquet.dtmeta`)
- Explicit compression level rejects with `r(198)`
- `compress_string_to_numeric` remains unsupported

## Constraints

| Constraint | Rule |
|------------|------|
| Command syntax | Keep unchanged |
| Edits | Minimal, localized, reversible |
| Tests | Keep deterministic, no weakening |
| Dependencies | No external tooling additions |
| ABI | Don't rename `_stata_` or change symbols |
| New subcommands | Only with ADO wiring + tests |

## Current State

### Existing Module Structure

```
plugin/src/
├── lib.rs              # 491 lines - Entry point, CLI parsing, command dispatch
├── read.rs             # 1960 lines - Parquet reading, data transfer to Stata
├── write.rs            # 1024 lines - Stata to Parquet writing
├── utilities.rs        # 376 lines - Thread pools, batch tuning, time constants
├── mapping.rs          # ~150 lines - Polars ↔ Stata type mapping
├── if_filter.rs       # 1056 lines - SQL filter compilation (contains FilterTranslator)
├── metadata.rs         # ~200 lines - DtMeta handling in Parquet metadata
├── boundary.rs         # ~200 lines - Schema handoff between Stata macros
├── downcast.rs         # ~100 lines - Type downcasting for Polars
├── stata_interface.rs # ~261 lines - FFI with Stata
# Note: sql_from_if.rs does NOT exist - SQL translation is in if_filter.rs
```

### Problem Analysis

1. **Monolithic Files**: `read.rs` exceeds 1900 lines, containing multiple distinct responsibilities
2. **Code Duplication**: Metrics, batch tuning, and filter handling are duplicated across read/write
3. **Tight Coupling**: Data transfer logic is embedded within file I/O code
4. **Testing Difficulty**: Large files make unit testing impractical
5. **Onboarding**: New developers must understand the entire codebase to make changes
6. **Error Handling**: Inconsistent error types across modules (Box<dyn Error>, CommandError, PolarsError)

## Target Architecture

```
plugin/src/
├── lib.rs                    # Thin entry point (target: ~200 lines)
│
# New consolidated modules (FLAT structure - no subdirectories)
├── error.rs                 # Unified error handling (~80 lines)
├── config.rs                # Constants and configuration (~100 lines)
├── transfer.rs              # Data transfer (~500 lines, reader+writer)
├── plan.rs                 # Scan planning (~300 lines)
├── schema.rs               # Schema introspection (~150 lines)
├── filter.rs               # Filter expressions (~1000 lines, from if_filter.rs)
├── metrics.rs              # Metrics infrastructure (~150 lines)
│
# Existing modules (unchanged or refactored)
├── read.rs                 # Orchestration only (target: ~400 lines)
├── write.rs                # Orchestration only (target: ~400 lines)
├── utilities.rs            # Enhanced with temporal module
├── mapping.rs             # Type mapping
├── if_filter.rs            # Wrapper for backward compat: `pub use crate::filter::*`
├── metadata.rs            # DtMeta handling
├── boundary.rs            # Schema handoff
├── downcast.rs           # Type downcasting
└── stata_interface.rs    # FFI with Stata
```

### Why Flat Structure (Per Kimi's Review)

- **Cognitive load**: Low - "1 file = 1 concern"
- **Navigation**: Simple - no nested directories
- **Compile time**: Faster - fewer mod.rs files
- **Backward compatibility**: Maintained via re-exports in original files

## Slice Dependency Analysis

### Independence Classification

| Slice | Type | Rationale |
|-------|------|-----------|
| **7.1.1** Temporal utilities | **INDEPENDENT** | No new modules, only reorganization |
| **7.1.2** Metrics infrastructure | **INDEPENDENT** | Creates traits only, no code moves |
| **7.1.3** Batch mode organization | **INDEPENDENT** | Documentation reorganization only |
| **7.2.1** Create transfer module | **SEQUENTIAL** | Must create module structure first |
| **7.2.2** Extract reader transfer | **SEQUENTIAL** | Depends on 7.2.1 (module must exist) |
| **7.2.3** Extract writer transfer | **INDEPENDENT** | Different file, can parallel with 7.2.2 |
| **7.2.4** Refactor read.rs | **SEQUENTIAL** | Depends on 7.2.2 (uses extracted code) |
| **7.2.5** Refactor write.rs | **SEQUENTIAL** | Depends on 7.2.3 (uses extracted code) |
| **7.3.1** Create plan module | **SEQUENTIAL** | Must create module structure first |
| **7.3.2** Extract read planning | **SEQUENTIAL** | Depends on transfer module (needs TransferColumnSpec) |
| **7.3.3** Extract write planning | **INDEPENDENT** | Different file, can parallel with 7.3.2 |
| **7.4.1** Create schema module | **SEQUENTIAL** | Must create module structure first |
| **7.4.2** Create filter module | **INDEPENDENT** | if_filter.rs already exists, can work in parallel |
| **7.5.1** Thin entry point | **SEQUENTIAL** | Depends on all other slices being complete |
| **7.5.2** Command module | **SEQUENTIAL** | Depends on 7.5.1 |

### Parallel Execution Opportunities

```
INDEPENDENT TASKS THAT CAN RUN IN PARALLEL:
- 7.1.1, 7.1.2, 7.1.3 (any order)
- 7.2.2 and 7.2.3 (after 7.2.1)
- 7.3.2 and 7.3.3 (after 7.3.1)
- 7.4.2 can start anytime after foundation

CRITICAL PATH (MUST BE SEQUENTIAL):
7.1 → 7.6 → 7.2.1 → 7.2.2/7.2.3 → 7.2.4/7.2.5 → 7.3.1 → 7.3.2/7.3.3 → 7.4.1 → 7.5.1 → 7.5.2
              ↑
         (Error/Config must come before Transfer)
```

## Implementation Slices

### Slice 7.1: Foundation (Low Risk) - ALL INDEPENDENT

**Objective**: Extract utilities and establish shared infrastructure.

#### 7.1.1 Temporal Utilities
- Move time/date constants to dedicated section in `utilities.rs`
- Extract conversion functions used by both read and write
- No new files
- **Status**: INDEPENDENT

#### 7.1.2 Metrics Infrastructure
- Create `metrics.rs` with shared trait definitions
- Extract `RuntimeMetrics` and `BatchTunerMetrics` traits
- Keep inline implementations in read/write during transition
- **Status**: INDEPENDENT

#### 7.1.3 Batch Mode Organization
- Clarify `BatchMode` documentation
- Group `WritePipelineMode` with related functions
- **Status**: INDEPENDENT

**Validation**: Run tests 1-7 after completing all 7.1 sub-slices.

### Slice 7.2: Data Transfer Layer (Medium Risk)

**Objective**: Extract core data conversion logic into testable module.

**CRITICAL: Decoupling Stata FFI** (per Gemini feedback): 
- Many functions in read.rs (like `write_numeric_column_range`) call `replace_number` directly from `stata_interface`
- If these are moved to transfer/ as-is, they remain untestable without Stata
- **Solution**: Use an intermediate buffer or trait-based sink:
```rust
// Define trait for testability
pub trait TransferSink {
    fn write_numeric(&mut self, row: usize, col: usize, value: Option<f64>);
    fn write_string(&mut self, row: usize, col: usize, value: Option<String>);
}

// Production implementation uses stata_interface
pub struct StataSink { ... }
impl TransferSink for StataSink { ... }

// Mock for unit testing
pub struct MockSink { ... }
impl TransferSink for MockSink { ... }
```

**Orchestration Separation** (per Gemini feedback):
- `sink_dataframe_in_batches` is complex orchestration
- It should NOT move to transfer/ if it depends on ReadScanPlan (in plan/)
- **Solution**: Keep orchestration in read.rs; transfer/ only handles data conversion
- transfer.rs accepts DataFrame, outputs rows via TransferSink

#### 7.2.1 Create transfer.rs (FLAT structure per Kimi's review)
```
transfer.rs (~500 lines)
├── pub mod reader { }  # Polars → Stata conversion
└── pub mod writer { }  # Stata → Polars conversion
```
- **Status**: SEQUENTIAL (must be first)
- **Depends on**: Slice 7.1 complete

#### 7.2.2 Extract Reader Transfer (~400 lines from read.rs)
- `TransferColumnSpec` struct
- All `convert_*` functions (15 functions)
- `write_numeric_column_range`, `write_string_column_range`
- `sink_dataframe_in_batches` - Uses `TransferColumnSpec` (not ReadScanPlan), safe to move to transfer.rs
- **Status**: SEQUENTIAL
- **Depends on**: 7.2.1

**Can parallel with**: 7.2.3 (after 7.2.1)

#### 7.2.3 Extract Writer Transfer (~300 lines from write.rs)
- `ExportField` struct
- `read_batch_from_columns`
- `series_from_stata_column`
- `validate_stata_schema`
- **Status**: INDEPENDENT (after 7.2.1)
- **Depends on**: 7.2.1 only

#### 7.2.4 Refactor read.rs
- Import from `transfer::reader`
- Remove extracted code
- Keep orchestration logic
- **Status**: SEQUENTIAL
- **Depends on**: 7.2.2

#### 7.2.5 Refactor write.rs
- Import from `transfer::writer`
- Remove extracted code
- Keep orchestration logic
- **Status**: SEQUENTIAL
- **Depends on**: 7.2.3

**Validation**: Run tests 1-7 after completing 7.2.

### Slice 7.3: Scan Planning (Medium Risk)

**Objective**: Extract scan plan construction. Using flat file structure per Kimi's review.

#### 7.3.1 Create plan.rs
```
plan.rs (~300 lines)
├── pub mod read { }  # ReadScanPlan
└── pub mod write { } # WriteScanPlan
```
- **Status**: SEQUENTIAL
- **Depends on**: Slice 7.2 complete

#### 7.3.2 Extract Read Planning (~200 lines)
- `ReadScanPlan`, `ReadBoundaryInputs`
- `build_read_scan_plan`
- `resolve_read_boundary_inputs`
- **Status**: SEQUENTIAL
- **Depends on**: 7.3.1, requires transfer module

**Can parallel with**: 7.3.3 (after 7.3.1)

#### 7.3.3 Extract Write Planning (~200 lines)
- `WriteScanPlan`, `WriteBoundaryInputs`
- `build_write_scan_plan`
- `resolve_write_boundary_inputs`
- **Status**: INDEPENDENT (after 7.3.1)
- **Depends on**: 7.3.1 only

**Validation**: Run tests 1-7 after completing 7.3.

### Slice 7.4: Schema and Filter (Lower Risk)

#### 7.4.1 Create schema.rs
```
schema.rs (~150 lines)
```
- Extract `file_summary`, `set_schema_macros`
- Extract `validate_parquet_schema`, `sample_parquet_schema`
- **Status**: SEQUENTIAL
- **Depends on**: Slice 7.3 complete

#### 7.4.2 Create filter.rs
```
filter.rs (~1000 lines, from if_filter.rs)
```
- **NOTE**: `if_filter.rs` is **1056 lines** (the entire SQL translation logic)
- Move ALL content from `if_filter.rs` to `filter.rs`, **including `convert_if_sql`**
- **NO separate `sql_from_if.rs`**: SQL translation stays in `filter.rs` as part of consolidated module
- Keep wrapper in `if_filter.rs`: `pub use crate::filter::*;`
- **Status**: INDEPENDENT
- **Can start**: After 7.1, parallel with other work

**Validation**: Run tests 1-7 after completing 7.4.

### Slice 7.5: Library Refactoring (REQUIRED - Not Optional)

**Objective**: Thin the entry point.

#### 7.5.1 Thin Entry Point
- Move argument parsing to `parse/` module
- Target: ~200 lines in `lib.rs`
- **Status**: SEQUENTIAL
- **Depends on**: All previous slices complete

#### 7.5.2 Command Dispatch
- Extract command dispatch functions into `lib.rs` or separate into:
  - `commands.rs` (~300 lines) - Flat file with `handle_read`, `handle_write`, `handle_describe`
- **NO nested directories** - maintain flat structure consistency
- **Status**: SEQUENTIAL
- **Depends on**: 7.5.1

**Validation**: Run tests 1-7 after completing 7.5.

### Slice 7.6: Error and Config (MOVED UP - Foundation)

**Objective**: Add missing modules. Moved earlier per Gemini recommendation. Using flat file structure per Kimi's review.

#### 7.6.1 error.rs
```
error.rs (~80 lines)
```
- Unified error enum using **manual `From` implementations** (no external deps)
- **NOTE**: `thiserror` not available per "No external tooling additions" constraint
- Stata error code mapping
- From implementations for existing error types
- **Status**: FOUNDATION (do first after 7.1)

#### 7.6.2 config.rs
```
config/mod.rs
config/constants.rs
```
- Centralize magic numbers
- Batch size defaults
- Thread count configurations
- Buffer sizes
- **Status**: FOUNDATION (do first after 7.1, parallel with 7.6.1)

## Revised Implementation Order

Based on Gemini feedback, Slice 7.6 (Error & Config) is moved to execute immediately after Slice 7.1 to establish foundational types before refactoring larger modules.

```
Phase 7.1: Foundation (all independent)
├── 7.1.1 Temporal utilities
├── 7.1.2 Metrics infrastructure
└── 7.1.3 Batch mode organization

Phase 7.6: Error and Config (MOVED UP - establishes types for all modules)
├── 7.6.1 Error module                  [Foundation - do first]
└── 7.6.2 Config module                 [Foundation - do first]

Phase 7.2: Transfer Layer
├── 7.2.1 Create transfer module          [SEQUENTIAL - first]
├── 7.2.2 Extract reader transfer        [SEQUENTIAL - after 7.2.1]
├── 7.2.3 Extract writer transfer       [INDEPENDENT - after 7.2.1, parallel with 7.2.2]
├── 7.2.4 Refactor read.rs              [SEQUENTIAL - after 7.2.2]
└── 7.2.5 Refactor write.rs             [SEQUENTIAL - after 7.2.3]

Phase 7.3: Planning Layer
├── 7.3.1 Create plan module            [SEQUENTIAL - first]
├── 7.3.2 Extract read planning         [SEQUENTIAL - after 7.3.1, needs transfer]
└── 7.3.3 Extract write planning        [INDEPENDENT - after 7.3.1, parallel with 7.3.2]

Phase 7.4: Schema and Filter
├── 7.4.1 Create schema module           [SEQUENTIAL - after 7.3]
└── 7.4.2 Create filter module           [INDEPENDENT - can parallel]

Phase 7.5: Library Refactoring (REQUIRED)
├── 7.5.1 Thin entry point              [SEQUENTIAL - after all above]
└── 7.5.2 Command module                [SEQUENTIAL - after 7.5.1]
```

## Explicit Dependency Rules

```rust
// In each module's mod.rs, include:
//! Module dependencies:
//! - Required: [list]
//! - Optional: [list]
//! - Not allowed: [list]

// Example for transfer/mod.rs:
//! Module dependencies:
//! - Required: mapping, utilities, stata_interface
//! - Optional: error
//! - Not allowed: read, write, plan, schema
```

### Dependency Matrix

| Module | Depends On | Used By | Conflict Risk |
|--------|-----------|---------|---------------|
| error/ | utilities | all | None |
| config/ | utilities | all | None |
| transfer/reader | mapping, utilities, stata_interface | read | Low |
| transfer/writer | mapping, utilities, stata_interface | write | Low |
| metrics/ | utilities | read, write | None |
| plan/read | boundary, transfer, mapping | read | Low |
| plan/write | boundary, transfer, mapping | write | Low |
| schema/ | utilities, stata_interface | read | Low |
| filter/ | none (self-contained) | read, write | None |

## Risk Mitigation

### Risk 1: Breaking Public API
- Use `pub use` re-exports in original module files
- Maintain function signatures exactly
- Run integration tests after each slice

### Risk 2: Circular Dependencies
- Follow: `lib.rs` → `read/write` → `transfer/plan/schema` → `utilities`
- Keep `utilities` free of business logic
- Use traits for cross-module communication

### Risk 3: Performance Regression
- **NEW**: Run baseline benchmarks before starting
- **NEW**: Define acceptance threshold (<5% regression)
- Extract logic without rewriting
- Preserve existing data flow

### Risk 4: Scope Creep
- Complete each slice before starting next
- Document decisions in code comments
- Review after each slice for course correction

### Risk 5: Slice Failure Rollback
- **NEW**: Keep extracted code in separate files from start
- **NEW**: Use feature flags for gradual rollout if needed
- **NEW**: Create git checkpoint after each successful slice

## Performance Baseline

Before starting Phase 7, establish baseline:

```bash
# Run tests and record timing
time StataMP-64.exe -e do dtparquet_test1.do
time StataMP-64.exe -e do dtparquet_test2.do
# ... etc
```

Define acceptance:
- <5% regression: Accept
- 5-10% regression: Investigate
- >10% regression: Rollback and re-analyze

## Alternative: Simplified Flat Structure (Per Kimi's Review)

Kimi's review suggested a simpler flat structure instead of nested directories:

| Aspect | Nested Plan | Flat Plan |
|--------|-------------|-----------|
| Files | 25+ | **15** |
| Directories | 8 | **0** |
| mod.rs boilerplate | 8 | **0** |
| Navigation | Complex | **Simple** |

**Key simplifications (per Kimi's review):**
1. `error/mod.rs + stata.rs` → `error.rs`
2. `transfer/reader.rs + writer.rs` → `transfer.rs` (with `pub mod reader`)
3. `plan/read.rs + write.rs` → `plan.rs`
4. `schema/describe.rs + validation.rs` → `schema.rs`
5. `filter/expr.rs` → `filter.rs`

**This plan uses the flat structure** as updated in Target Architecture above.

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| `read.rs` lines | 1960 | ~400 |
| `write.rs` lines | 1024 | ~400 |
| `lib.rs` lines | 490 | ~200 |
| Modules with tests | 0 | 8+ |
| Average function length | ~50 | ~30 |
| Error types | 4+ inconsistent | 1 unified |

## Policy Invariants (Must Preserve)

| Policy | Current Behavior | Must Not Change |
|--------|-----------------|----------------|
| Metadata | In-parquet only (`dtparquet.dtmeta`) | Must remain |
| Compression level | Explicit level rejects with r(198) | Must remain |
| compress_string_to_numeric | Unsupported | Must remain |

## Verification Checklist (Per Kimi's Review)

- [ ] Each file < 500 lines (except filter.rs ~1000)
- [ ] No directories deeper than 1 level
- [ ] `pub use` re-exports maintain backward compatibility
- [ ] FFI symbols unchanged (_stata_*)
- [ ] All 7 tests pass after each phase
- [ ] Performance regression < 5%

## Testing Strategy

### Unit Tests

Each new module should have corresponding tests (inline in flat files):

```rust
// In transfer.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_numeric_column() {
        // Test with MockStataSink
    }
}

// In plan.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_build_read_plan() { ... }
}

// In filter.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_convert_if_sql() { ... }
}
```

### Integration Tests

- Existing tests 1-7 must continue to pass
- Add new tests for edge cases discovered during extraction
- Run full suite after each slice completion

### Policy Invariant Tests

Add unit tests to verify policy invariants are preserved (per Gemini feedback):
```rust
// In config.rs
#[cfg(test)]
mod policy_tests {
    #[test]
    fn test_metadata_parquet_only() {
        // Verify metadata only stored in parquet
    }

    #[test]
    fn test_explicit_compression_rejected() {
        // Verify explicit level rejects r(198)
    }

    #[test]
    fn test_compress_string_unsupported() {
        // Verify feature remains unsupported
    }
}
```

## Review Adjustments Made

Based on subagent feedback:

1. **Split Slice 7.2** into 5 sub-slices (was 4)
2. **Made Slice 7.5 required** (was "if time permits")
3. **Added error module** to target architecture
4. **Added config module** to target architecture
5. **Adjusted for if_filter.rs size** (1056 lines, not 400)
6. **Added explicit dependency rules** and matrix
7. **Added performance baseline** with acceptance thresholds
8. **Identified parallel execution opportunities**
9. **Added Slice 7.6** for error and config modules

Based on Gemini feedback:

10. **Moved Slice 7.6 earlier** - After 7.1, before 7.2 (establishes foundational types)
11. **Fixed sql_from_if.rs reference** - Clarified SQL logic is in if_filter.rs
12. **Added decoupling guidance** - Traits for transfer module testability
13. **Added policy invariant tests** - Unit tests for metadata/compression policies

## References

- HANDOFF.md: Current working constraints and validation workflow
- Phase 2 Slices 1-5: Completed modularization work
- dtparquet_test1.do through dtparquet_test7.do: Validation tests
