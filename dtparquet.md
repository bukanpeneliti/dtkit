# Revised implementation plan: dtparquet (consolidated SFI architecture)

## Scope and invariants

- All data I/O uses SFI. No `pandas.read_stata` path exists.
- `dtparquet` targets lossless round-tripping between Stata and Parquet.
  - When true lossless round-tripping is not feasible for a storage type,
    `dtparquet` fails fast unless an explicit lossy option exists.
- Value labels do not replace underlying values.
  - `dtparquet` always stores the underlying numeric codes.
  - `dtparquet` optionally stores and restores label definitions as metadata.
- Metadata lives in Parquet file key-value metadata (stored in the Parquet
  footer).
  - Key: `dtparquet.dtmeta`
  - Value: JSON
  - Versioning: `dtparquet.dtmeta.schema_version` inside the JSON
- The MVP includes full metadata preservation via `dtmeta` integration.

## Phase 1: Core Engine & Memory I/O

Objective: Perfect the "Golden Rule" type system and basic Stata memory operations
(Memory ↔ Parquet). Prove data fidelity before adding complexity.

### strL mapping protocol

- **Feasibility gate**: Confirm that SFI exposes `strL` values in a form that
  permits binary-safe round-tripping.
  - If SFI only exposes decoded text, treat `strL` as text. Do not claim blob
    support.
- **Default**: Map `strL` to Parquet `Binary` (BYTE_ARRAY) only when SFI
  exposes raw bytes.
- **Optional Enforcement**: Add a `text_strl` option. If enabled, the Python
  writer attempts to decode `strL` to UTF-8. If successful, store as Parquet
  `String`; if failed, fallback to `Binary` and log a warning.
- **Metadata Flag**: `dtparquet.dtmeta.dtvars` records `strl_content_type`
  ("text" or "binary") for restoration.

### Datetime preservation strategy

- **Storage**: Stata dates (integer/double) are stored as **Arrow Int64**
  (or Double) to preserve the exact raw value.
- **Interpretation**: No "on-the-fly" conversion to Parquet Timestamp. The
  specific Stata format string (e.g., `%tc`, `%td`, `%tC`) is preserved in
  `dtparquet.dtmeta.dtvars`.
- **Restoration**: Upon load, the raw number is restored, and the format is
  re-applied via Stata's `format` command. This delegates date logic entirely
  to Stata, avoiding timezone skew or epoch mismatches in Python.

### Unified SFI I/O logic (the SFI standard)

- **Decision**: Abandon `pandas.read_stata` for `export`.
- **New Rule**: All operations use the **same** SFI-based extraction logic.
  `export` (Phase 2) will simply load chunks into a temporary Stata frame
  and trigger the `save` routine on that frame. This ensures 100% consistency
  in how data is interpreted, regardless of source.

### Missing values policy

- Integer storage types preserve Stata missing codes by storing raw numeric
  values.
- String missing stays the empty string (Stata missing for strings). The
  writer does not emit Parquet nulls for Stata strings.
- Float/double extended missing identity is not guaranteed across Parquet
  readers and writers.
  - Default: fail fast when float/double contains extended missing codes.
  - Optional: add a lossy mode that collapses extended missing to system
    missing and records the loss in `dtparquet.dtmeta.warnings`.

### Value label policy

- Default: store numeric codes in Parquet.
- Default: use `dtmeta` to capture value label definitions and re-apply
  them on load.
- `nolabel`: do not write or apply value label metadata.

### Command Interface (Memory Only)

**Dependency checks (dtparquet.ado):**

- Stata version ≥ 16.0 (sfi requires it)
- `dtmeta.ado` (required for metadata handling)
- `python which pyarrow` for external dependency

**dtparquet save filename [, replace nolabel]**

- **Source:** Current Stata memory (Frame).
- **Target:** Parquet file.
- **Options:**
  - `replace`: Overwrite existing file.
  - `nolabel`: Do not write value label metadata.

**dtparquet use [varlist] [if] [in] using filename [, clear nolabel]**

- **Source:** Parquet file.
- **Target:** Current Stata memory (Frame).
- **Features:**
  - `[varlist]`: Load only specific columns (efficient, as Parquet is columnar).
  - `[if] [in]`: Filter rows after loading. Complex `if` expressions are
    evaluated in Stata (not Python) to ensure correct handling of Stata
    functions and syntax. **Note**: For large datasets, filtering in Python would
    be more efficient, but Stata `if` expressions are not reliably parsable.
- **Options:**
  - `clear`: Clear current data before loading.
  - `nolabel`: Do not apply value label metadata.

### Technical Refinements (from Phase 1 Implementation)

- **SFI Frame Management**: Python logic must use `sfi.Frame().name` to capture the current state and `sfi.Frame(target).changeToCWF()` to switch contexts. This ensures metadata extraction and application occur in the correct frame without corrupting the active dataset.
- **Stata `syntax` Workaround**: `dtparquet use` employs manual parsing (`gettoken`) for the `varlist` and `using` path. Stata's native `syntax [varlist]` fails when memory is empty; manual parsing permits loading data into a fresh Stata session.
- **Python Type Bridge**: Since SFI lacks a generic `addVar(type, name)` method, the Python layer implements a typed helper (`add_stata_var`) that maps Arrow/JSON types to specific SFI methods (`addVarByte`, `addVarDouble`, `addVarStrL`, etc.).
- **Windows Path Normalization**: The ADO layer normalizes all file paths by converting backslashes (`\`) to forward slashes (`/`) before passing them to the Python environment to prevent escape-character corruption.
- **Label Restoration Safety**: `dtparquet use` utilizes `tempname` frames to isolate value label definitions during restoration. This prevents collisions and ensures that labels are correctly associated with variables even when loading a subset (`varlist`).

### Phase 1 outputs

**Files:** `ado/dtparquet.ado`, `ado/dtparquet.py`,
`ado/ancillary_files/test/dtparquet/dtparquet_test1.do`

**dtparquet.ado:** Defines `dtparquet save` (save to Parquet) and
`dtparquet use` (load from Parquet) commands. Calls `dtmeta` before save.

**dtparquet.py:** Implements type mapping between Stata and Arrow/Parquet
types, strL handling, and core save/load functions.

**Functions:**

- `stata_to_arrow_type()` - Maps Stata storage types to Arrow types
- `arrow_to_stata_type()` - Maps Arrow types back to Stata storage types
- `handle_strl()` - Processes strL values for Parquet Binary/String storage
- `extract_dtmeta()` - Captures `_dt*` frames generated by `dtmeta.ado` into JSON
- `apply_dtmeta()` - Restores `_dt*` frames from JSON for Stata-side application
- `save()` - Saves current Stata dataset to Parquet file
- `load()` - Loads Parquet file into Stata

**Test file:** `dtparquet_test1.do` contains tests for basic save/load,
strL handling, datetime preservation, and `varlist` subsetting.

## Phase 2: Disk Wrappers & Robustness

Objective: Complete the interface with disk-to-disk operations and ensure
operations are safe, self-validating, and atomic.

### Disk Command Interface

**dtparquet export dta_filename using parquet_filename [, replace nolabel]**

- **Source:** `.dta` file on disk.
- **Target:** Parquet file on disk.
- **Implementation:** Loads `.dta` into a **temporary hidden Frame**, runs the
  `save` logic, then destroys the frame.
- **Safety:** Does **not** touch your current active dataset.

**dtparquet import parquet_filename using dta_filename [, replace nolabel]**

- **Source:** Parquet file on disk.
- **Target:** `.dta` file on disk.
- **Implementation:** Loads Parquet into a **temporary hidden Frame**, runs
  Stata's native `save`, then destroys the frame.
- **Safety:** Does **not** touch your current active dataset.

### Atomic write protocol (.tmp strategy)

- **For `save` / `export`**: Write to `filename.parquet.tmp`. Only rename to
  `filename.parquet` after the file footer (metadata) is successfully written
  and closed.
- **For `import`**: Write to `filename.dta.tmp`. Rename only after the final
  metadata application step succeeds.
- **Rollback**: In case of python-side exception, a `try/finally` block
  ensures the `.tmp` file is deleted, leaving the user's filesystem clean.
- **Fail-Fast**: Any error aborts the entire operation immediately. No partial
  writes or graceful degradation.

### Version compatibility gate

- Define `min_stata_version = 16.0` (required for Frames).

- **Default**: Silent mode (errors only)
- **Options**: `verbose` flag enables detailed diagnostics and warnings
- **Progress**: Progress indicators always shown for operations >1000
  observations

### Phase 2 outputs

**Files:** `ado/dtparquet.ado` (updated), `ado/dtparquet.py` (updated),
`ado/ancillary_files/test/dtparquet/dtparquet_test1.do` (updated)

**dtparquet.ado:** Adds version checks, Python dependency validation, and
`dtparquet export` / `dtparquet import` commands.

**dtparquet.py:** Adds dependency checking, version compatibility validation,
atomic write protocol with .tmp files, temp frame management, and progress
logging.

**Functions (new):**

- `check_dependencies()` - Validates Python environment
- `get_stata_version()` - Retrieves Stata version via SFI
- `check_file_version()` - Validates Parquet file metadata compatibility
- `save_atomic()` - Atomically writes Parquet using .tmp strategy
- `load_atomic()` - Atomically loads Parquet to .dta using .tmp strategy
- `export_via_temp_frame()` - Implements export via temp frame wrapper
- `import_via_temp_frame()` - Implements import via temp frame wrapper

**Test file:** Updated `dtparquet_test1.do` includes tests for atomic write
rollback, version compatibility, and export/import wrappers.

## Phase 3: Enriched standard metadata engine

Objective: Preserve full Stata metadata and emit an interoperable Parquet
schema. Treat `dtparquet.dtmeta` as enrichment, not as a substitute for a sane
schema.

### Compression configuration

- **Default**: No compression (uncompressed)
- **Rationale**: Maximum write speed, simplest interoperability
- **Future**: Users can add `compression=snappy|gzip|zstd` via pyarrow writer
  options if needed

### Dual-layer metadata

- **Layer 1 (Standard)**: Write variable names and storage types (mapped to
  closest Arrow equivalent) into the standard Parquet Schema. This ensures the
  file is readable by Spark, DuckDB, etc., even if they strip custom metadata.
- **Layer 2 (Enrichment)**: `dtparquet.dtmeta` contains Stata-specific
  attributes (value labels, notes, characteristics, precise formats).

### Schema evolution logic

- The JSON includes `schema_version` and `min_reader_version`.
- If the reader encounters unknown keys, it ignores them.
- If `min_reader_version` exceeds the installed version, the reader aborts.

### Phase 3 outputs

**Files:** `ado/dtparquet.py` (updated),
`ado/ancillary_files/test/dtparquet/dtparquet_test1.do` (updated)

**dtparquet.py:** Refines metadata handling to ensure full capture and
restoration of all four `dtmeta` frames, constructs dual-layer Parquet schemas,
and handles standard metadata read/write.

**Functions (new):**

- `build_arrow_schema()` - Constructs Parquet schema from Stata variables
- `write_standard_parquet()` - Writes Parquet with standard schema + metadata
- `read_standard_parquet()` - Reads Parquet extracting data and custom
  metadata
- Refined `extract_dtmeta()` and `apply_dtmeta()` for full frame serialization

**Test file:** Updated `dtparquet_test1.do` includes tests for metadata
preservation (data label, variable notes, value labels).

## Phase 4: Unified streaming architecture (the buffer frame) [COMPLETED]

Objective: Solve the "Memory vs. Disk" conflict by using Stata Frames as a
standardized sliding window.

### The buffer frame pattern

**`export` (Disk .dta $\to$ Parquet):**

- Loop: `use [varlist] in [start]/[end] using "source.dta", clear` (Load
  chunk to Frame).
- Call **SFI Writer** (see Phase 5) on active Frame.
- Clear Frame.

**`import` (Parquet $\to$ Disk .dta):**

- Create empty target `.dta` (using `sfi.Data.addVar` + `save`).
- Loop: Read Parquet chunk (Python) $\to$ Push to "Buffer Frame" (SFI) $\to$
  `append` Buffer Frame to target `.dta`.
- **Performance Note**: This accepts the I/O overhead of `append` in exchange
  for strictly capped memory usage.

### Memory-boundedness

- Both `import` and `export` are now strictly bound by `chunksize`. RAM usage
  never exceeds `chunksize * row_width`.

### Phase 4 outputs

**Files:** `ado/dtparquet.ado` (updated), `ado/dtparquet.py` (updated),
`ado/ancillary_files/test/dtparquet/dtparquet_test1.do` (updated)

**dtparquet.ado:** Adds streaming commands `dtparquet_export_large` and
`dtparquet_import_large` for large dataset handling.

**dtparquet.py:** Adds streaming functions for large dataset export/import,
buffer frame management, and chunk-based Parquet I/O.

**Functions (new):**

- `export_stream()` - Streams large .dta to Parquet using buffer frames
- `load_stream()` - Streams large Parquet to .dta using buffer frames
- `create_empty_dta()` - Creates empty .dta with variable definitions
- `append_frame_to_dta()` - Appends buffer frame to target .dta file

**Test file:** Updated `dtparquet_test1.do` includes tests for streaming
export/import of large datasets (100K observations).

## Phase 5: Optimized SFI streaming [COMPLETED]

Objective: Implement the SFI bridge with row-major chunking to prevent memory
blowouts on wide data.

### Row-block chunking (the correct save loop)

- Instead of `get(var_index)` (Column-major, unsafe for wide data), use a
  Row-major strategy.

**Loop:**

1. Define a row range `i` to `i+N`.
2. **Python List Construction**: `data = sfi.Data.get(var_indices, i, i+N)`
   (Fetches a list of tuples).
3. **Transposition**: Convert list-of-tuples to `pyarrow.Table` (Arrow handles
   this transposition efficiently).
4. **Write**: Stream Table to Parquet.

- This balances context-switching (one call per chunk) with memory safety
  (only N rows in memory).

### Symmetric UTF-8 handling

- **In Python (Writer)**: Iterate the tuple list. If a column is string
  type, explicitly run `.encode('utf-8')` on the elements *if* the source
  Stata encoding is not confirmed to be UTF-8 (legacy safety).
- **In Python (Reader)**: When reading Parquet strings, explicitly
  `.decode('utf-8')` before passing to `sfi.Data.store`.

### Tunable chunksize

- **Hybrid Strategy**: Auto-detect based on available RAM and row width, with
  manual override
- **Default**: 50,000 observations
- **Auto-detection formula**: `chunksize = min(100000, floor(available_RAM /
  (row_width * 4)))`
- **Override**: User can specify `chunksize(#)` parameter to force custom size

### Phase 5 outputs

**Files:** `ado/dtparquet.py` (updated with optimized SFI),
`ado/ancillary_files/test/dtparquet/dtparquet_test1.do` (updated)

**dtparquet.py:** Adds row-major SFI chunking functions, UTF-8 encoding, and
optimized save/load functions.

**Functions (new):**

- `save_frame_row_major()` - Saves Stata frame using row-major chunking
- `load_parquet_to_frame()` - Loads Parquet chunk into Stata frame via SFI
- `encode_string_column()` - Encodes string column to UTF-8 for Parquet
- `decode_string_column()` - Decodes Parquet strings from UTF-8
- `get_optimal_chunksize()` - Calculates optimal chunksize
- `save_optimized()` - Optimized save with auto-detected chunksize
- `load_optimized()` - Optimized load with auto-detected chunksize

**Test file:** Updated `dtparquet_test1.do` includes tests for wide data
(row-major chunking) and UTF-8 encoding.

---

## Rejection of inefficiency critique for import

- **Critique**: "Append loop is slow."
- **Decision**: We accept this. The alternative (writing a binary `.dta`
- **Decision**: We accept this. The alternative (writing a binary `.dta`
  writer in Python) is out of scope and high-risk. The "Buffer Frame + Append"
  strategy is the only standard-compliant way to write `.dta` files larger than
  RAM. We will document this performance characteristic clearly: *"For massive
  imports, speed is limited by Stata's disk I/O. Use high-speed SSDs or increase
  chunksize."*

---

## Implementation guidance

### MVP (Minimum Viable Product)

The MVP is the smallest functional version that delivers core value. For
dtparquet, this means:

- Phase 1+2: Type system fidelity + robustness (production-ready core)
- Full 4-command interface (`save`, `use`, `import`, `export`) with fail-fast
  error handling
- Complete metadata preservation (Phase 3)
- No streaming/chunking optimization (Phase 4+ can follow later)

### Validation mode

A validation command would verify round-trip data integrity without writing
files:

- Checks that all values survive Parquet round-trip
- Verifies metadata preservation
- Tests type fidelity
- **Decision**: Not implementing validation command initially. Focus on robust
  `save`/`import` with comprehensive error reporting.

---

## Configuration decisions

### Compression

- **Default**: No compression (uncompressed Parquet)
- **Rationale**: Fastest write speed. Users can enable compression via pyarrow
  options if needed.

### Error handling

- **Policy**: Fail-fast (abort entire operation on first error)
- **Implementation**: All operations use strict error propagation. `.tmp`
  files are cleaned up on failure.

### Chunksize strategy

- **Mode**: Hybrid (auto-detect with manual override)
- **Default**: 50,000 observations
- **Auto-detection**: Calculate based on `available_RAM / (row_width *
  safety_factor)`, capped at 100,000
- **Override**: User can specify custom `chunksize(#)` parameter

### Metadata preservation

- **Scope**: Full preservation via `dtmeta` (value labels, notes, formats,
  data labels). *Note: Characteristics are not preserved as they are not
  part of the dtmeta output.*
- **Implementation**: All Stata metadata is captured from the `dtmeta` frames
  and stored in Parquet file metadata key `dtparquet.dtmeta`.

### Logging level

- **Default**: Silent (errors only)
- **Options**: `verbose` flag for detailed diagnostics, `silent` (default) for
  minimal output
- **Progress**: Always show progress indicators for large operations
