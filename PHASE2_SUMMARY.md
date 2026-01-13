# dtparquet Phase 2 Implementation Summary

## Overview

Phase 2 adds disk-to-disk operations with atomic write safety and temp frame isolation.

## Commands Added

### `dtparquet export`

Converts `.dta` file to Parquet format.

```
dtparquet export parquet_filename using dta_filename [, replace nolabel]
```

### `dtparquet import`

Converts Parquet file to `.dta` format.

```
dtparquet import dta_filename using parquet_filename [, replace nolabel]
```

## Key Features

### 1. Atomic Write Safety

- **Parquet files**: Python `.tmp` file strategy with `os.replace()`
- **.dta files**: Stata `tempfile` + `copy` command
- No partial/corrupted files on failure

### 2. Temp Frame Isolation

- Fresh `tempname` for each operation
- Original dataset unaffected
- Proper frame switching/restoration

### 3. Startup Cleanup

- Cleans orphaned `_dtparquet_*` frames
- Cleans orphaned `.parquet.tmp` files
- Automatic on every `dtparquet` command

### 4. Error Handling

- Stata: `capture` + `_rc` checking
- Python: `try...except` with cleanup
- Cross-boundary error propagation

## Implementation Details

### File Structure

- `ado/dtparquet.ado`: Updated with `export`/`import` commands
- `ado/dtparquet.py`: Added atomic write functions
- `test/dtparquet_test2.do`: Comprehensive test suite

### Core Functions (Python)

- `save_atomic()`: Atomic Parquet write via `.tmp` file
- `load_atomic()`: Wrapper for loading (atomicity in Stata)
- `cleanup_orphaned_tmp_files()`: Cleans up orphaned `.tmp` files

### Core Programs (Stata)

- `dtparquet_export`: Loads `.dta` → temp frame → exports to Parquet
- `dtparquet_import`: Loads Parquet → temp frame → saves to `.dta`
- `_cleanup_orphaned`: Cleans orphaned frames and `.tmp` files

## Testing

Comprehensive test suite (`dtparquet_test2.do`) includes:

1. Basic export/import round-trip
2. Metadata preservation
3. `nolabel` option
4. Replace protection
5. Frame isolation verification
6. Orphaned file cleanup
7. Error handling

## Performance Notes

- **Export**: Single I/O (`.dta` → Parquet)
- **Import**: Double I/O (Parquet → temp `.dta` → final `.dta`)
- **Memory**: Strictly bounded by dataset size (no streaming yet)

## Phase 1 Compatibility

- Existing `save`/`use` commands unchanged
- Same metadata handling via `dtmeta`
- Same type mapping system

## Next Steps (Potential Phase 3)

- Streaming for large datasets
- Compression options
- Enhanced schema metadata
- Performance optimizations

## Files Modified

1. `ado/dtparquet.ado` - Added export/import commands and cleanup
2. `ado/dtparquet.py` - Added atomic write functions
3. `test/dtparquet_test2.do` - New test suite

## Verification

Run verification script:

```bash
python verify_phase2_simple.py
```

Or run full test suite in Stata:

```stata
do ado/ancillary_files/test/dtparquet/dtparquet_test2.do
```
