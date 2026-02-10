# dtparquet Rust Refactor - Handoff

## Branch

`refactor/rust-dtparquet`

## Current State (as of commit `51f8447`)

- Rust Polars stack is upgraded to `0.52.0` (`polars`, `polars-sql`, and aligned
  split crates via lockfile).
- Upgrade rationale: keep current dtparquet behavior contracts while moving to the
  latest Polars API/runtime.
- Polars `0.52.0` compatibility edits were localized to path-typed scan/write
  callsites:
  - `LazyFrame::scan_parquet` now receives `PlPath::new(...)`.
  - `write_partitioned_dataset` now receives `PlPathRef::Local(...)`.
- No user-visible syntax or semantics were changed in ado/plugin contracts during
  this upgrade.

- `dtparquet use` is plugin-first and stable through batch regression.
- Ado pre-read macro contract is wired and consumed by Rust read path.
- `dtparquet save` is now plugin-first end-to-end (no Python save bridge).
- Rust save path (`plugin/src/write.rs`) now:
  - reads Stata data via plugin API (`read_numeric`, `read_string`, `read_string_strl`)
  - builds Polars columns/DataFrame
  - applies save-time `sql_if` filtering when provided
  - supports `partition_by` write path
  - enforces safe overwrite behavior for single-file and partitioned outputs
  - writes parquet with compression mapping (`lz4`, `uncompressed`, `snappy`,
    `gzip`, `lzo`, `brotli`, `zstd`)
  - embeds initial parquet metadata scaffold key `dtparquet.dtmeta`
  - writes atomically via `*.tmp` then rename
- Ado save path (`ado/dtparquet.ado`) now prepares macro contract for Rust save:
  - `varlist`, `var_count`
  - `name_*`, `dtype_*`, `format_*`, `str_length_*`
- Save/read-back regression is added in:
  - `ado/ancillary_files/test/dtparquet/dtparquet_test7.do`
- Targeted regression coverage added for:
  - partitioned save + overwrite guard behavior
  - save-time filter behavior
  - metadata key scaffold presence
- Build warning cleanup done:
  - bindgen callback API updated (`CargoCallbacks::new()`)
  - `ST_retcode` warning handled intentionally
  - unused metadata stub param warning removed
- Additional patch in this handoff cycle:
  - read-path batching/parallel internals were tuned in `plugin/src/read.rs`:
    - uses `get_thread_count()` instead of hard-coded single-thread batching.
    - eager-path batch count now uses realized sliced row count.
    - lazy-path batching exits early on first empty collected batch.
    - by-row chunk sizing now uses `max(512, row_count.div_ceil(threads * 8))`.
    - eager path now casts `categorical`/`enum` columns to string before Stata
      assignment so foreign dictionary/categorical values are not lost.
    - read path now reports deterministic `n_loaded_rows` so ado can trim
      preallocated observations when pushdown filtering returns fewer rows.
  - ado `dtparquet use` qualifier handling now accepts native `syntax [if] [in]`
    and trims to loaded rows, restoring deterministic `if` + `in` behavior.
  - compression-level contract is now deterministic on plugin save path:
    any explicit compression level (anything other than `-1`) is rejected with
    `r(198)`; no implicit fallback is applied.
  - Stata `if` expression translation is now wired in Rust read/save SQL filter
    path (`stata_to_sql` conversion before SQL execution).
  - ado `dtparquet use` now passes parsed `if` to plugin `sql_if` and no longer
    applies post-read `keep if/in` fallback.
  - `dtparquet_test7.do` now includes Test 5b to lock `if` qualifier pushdown
    behavior (`year > 2015` predicate assertions).
  - `dtparquet_use` now calls plugin `describe` with `detailed=1` to populate
    observed string lengths in `string_length_*` macros.
  - plugin schema mapping now defaults string-like columns to Stata `string`
    (with `strl` only when observed length exceeds 2045) to avoid empty-string
    loads from `strL` assignment path.
  - `dtparquet_gen_or_recast` now materializes `date` as `float` storage while
    keeping `%td` formatting, which restored native date/time roundtrip in
    `dtparquet_test5.do` Test 11.
  - metadata restore now uses in-parquet metadata key only
    (`dtparquet.dtmeta`); plugin `load_meta` reads metadata directly from the
    parquet bytes and repopulates macro contract for `dtparquet use`.
  - embedded `dtparquet.dtmeta` payload now also carries dataset notes and
    variable notes from `_dtinfo`/`_dtnotes` and reapplies them on
    `dtparquet use` when metadata is loaded.
  - embedded `dtparquet.dtmeta` now carries `_dtinfo` core fields
    (`dta_obs`, `dta_vars`, `dta_ts`) in addition to label/notes metadata.

## Validated Behavior

Use the latest pass/fail matrix in this file as source of truth.

Most recent one-by-one batch run (Polars `0.52.0`, 2026-02-10) has:

- pass: `dtparquet_test1.do` to `dtparquet_test7.do`
- fail: none

Latest run matrix from this cycle only (2026-02-10):

| File | Result | Failing cases |
| :--- | :--- | :--- |
| `dtparquet_test1.do` | pass | none |
| `dtparquet_test2.do` | pass | none |
| `dtparquet_test3.do` | pass | none |
| `dtparquet_test4.do` | pass | none |
| `dtparquet_test5.do` | pass | none |
| `dtparquet_test6.do` | pass | none |
| `dtparquet_test7.do` | pass | none |

## Known Gaps (Next Priority)

- Primary next objective: remove all Python runtime dependency from dtparquet.
- Active runtime touchpoints are plugin/Stata-native in `ado/dtparquet.ado`:
  - `dtparquet_export` and `dtparquet_import` no longer call Python bridges.
  - `_check_python` is a no-op for the active runtime path.
  - `_cleanup_orphaned` is Stata-frame cleanup only.
  - metadata key regression uses plugin call `has_metadata_key`.
- Legacy Python-based tests/scripts still exist in `ado/ancillary_files/test/dtparquet`
  and can be cleaned separately if no longer needed.
- Full metadata embedding/restoration (`_dtvars`, `_dtlabel`, `_dtnotes`, `_dtinfo`, value-label fidelity) is implemented for single-file saves.
  - *Note:* Metadata embedding for `partition_by` datasets is still pending.
- Save path is currently full in-memory DataFrame materialization (not chunked
  streaming write).

### Parity triage (next actions)

<!-- markdownlint-disable MD013 -->
| Item | Decision | Next action |
| :--- | :--- | :--- |
| `compress` save option parity | implemented | Keep deterministic checks for accepted values/defaults and reject explicit compression levels with `r(198)`. |
| `compress_string_to_numeric` parity | defer | Keep in backlog until plugin contract is finalized. |
| Full `_dt*` metadata parity | implemented (single-file) | Complete `partition_by` metadata embedding. |
<!-- markdownlint-enable MD013 -->

### Refactored runtime limitations observed in legacy suite

- All previously recorded failing capability gaps in `dtparquet_test1.do` to
  `dtparquet_test7.do` are now closed on this branch.
- `dtparquet_test5.do` includes intentional skips by test design:
  Test 5b (strL signature stress case) and legacy pyarrow-fixture-dependent
  tests (6, 7, 8, 9a, 9b, 10).

### Latest batch pass/fail matrix (2026-02-10)

Executed one-by-one in batch mode:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

<!-- markdownlint-disable MD013 -->
| File | Result | Failing cases |
| :--- | :--- | :--- |
| `dtparquet_test1.do` | pass | none |
| `dtparquet_test2.do` | pass | none |
| `dtparquet_test3.do` | pass | none |
| `dtparquet_test4.do` | pass | none |
| `dtparquet_test5.do` | pass | none |
| `dtparquet_test6.do` | pass | none |
| `dtparquet_test7.do` | pass | none |
<!-- markdownlint-enable MD013 -->

Latest rerun after read-path batching tuning and fixture-backed foreign
categorical coverage:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass. `dtparquet_test7.do` adds fixture-backed
foreign categorical checks with:

- `fixtures/foreign/foreign_cat_pandas.parquet` (`catmode(encode)`)
- `fixtures/foreign/foreign_cat_arrow_dict.parquet` (`catmode(raw|both)`)

Deterministic cleanup was rerun for `rust_roundtrip.parquet`,
`rust_filtered_save.parquet`, `rust_partitioned_out`, and `*.tmp` remnants.

Latest rerun details after crate alias rename (`parquet2` ->
`parquet_footer`) and lock-safe DLL promotion:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass in this rerun. Deterministic cleanup was
rechecked for `rust_roundtrip.parquet`, `rust_filtered_save.parquet`,
`rust_partitioned_out`, and `*.tmp` remnants.

Latest rerun after extending embedded metadata payload for notes
(`_dtinfo`/`_dtnotes`) and adding deterministic assertions in
`dtparquet_test7.do` Test 13:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; Test 13 confirms dataset label/notes and
variable notes restore from in-parquet metadata, while `nolabel` remains
deterministically metadata-suppressed.

Latest rerun after extending embedded metadata payload with `_dtinfo` core
fields (`dta_obs`, `dta_vars`, `dta_ts`) and adding explicit Test 13 asserts:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; metadata macros loaded from parquet now
include deterministic `_dtinfo` core fields in addition to label/notes.

Latest rerun after adding deterministic `compress()` save parity checks
(`zstd`, `uncompressed`, default behavior, invalid-value rejection):

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; `dtparquet_test7.do` now validates
`compress()` accepted values/default behavior and deterministic invalid input
handling (`r(198)`).

Latest rerun after adding explicit guardrail coverage that
`compress_string_to_numeric` remains unsupported in dtparquet syntax:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; `dtparquet_test7.do` now asserts
`compress_string_to_numeric` returns deterministic syntax error (`r(198)`).

### Explicit unsupported behavior (current)

- `dtparquet_test5.do` intentionally skips legacy pyarrow-fixture-dependent
  cases (6, 7, 8, 9a, 9b, 10) and strL stress case 5b by test design.
- `compress_string_to_numeric` is intentionally unsupported in dtparquet
  command syntax and is guarded by deterministic regression assertion (`r(198)`).

### Foreign categorical compatibility contract (current)

- For non-dtparquet parquet files where a column is observed as Polars
  `categorical`/`enum` and no `dtparquet.dtmeta` payload is loaded,
  `dtparquet use` converts the imported string values to numeric Stata variables
  with value labels via deterministic `encode` mapping.
- Label-set naming is deterministic per load (`dtpq_cat_1`, `dtpq_cat_2`, ... in
  matched variable order).
- Code-to-text mapping follows Stata `encode` semantics
  (sorted text order) and is stable for a fixed set of category strings.
- Existing in-parquet metadata restoration behavior remains unchanged and takes
  precedence whenever `dtparquet.dtmeta` is present (`dtmeta_loaded == 1`).
- No broad silent fallback was introduced: mapping is only applied to detected
  `categorical`/`enum` columns and still fails fast on invalid operations.
- `dtparquet use` now supports `catmode(encode|raw|both)` for foreign
  categorical handling when `dtparquet.dtmeta` is absent:
  - `encode` (default): replace variable with encoded numeric + value labels.
  - `raw`: keep string values unchanged, no generated label mapping.
  - `both`: keep string variable and add `<var>_id` (or `<var>_catid` on name
    collision) with deterministic value labels.

### Immediate next tasks

1. [x] Rename the Rust plugin workspace directory from `rust/` to `plugin/`.
2. Clean temporary/non-relevant generated files across the repo
   deterministically (not only `*.tmp`), excluding anything under
   `temp_repos/` and without deleting fixtures or source assets.
3. Produce an explicit track/commit decision list before release (what should be
   versioned now vs. kept untracked/machine-local).
4. [x] Update all dtparquet test do-files to deploy/copy local plugin DLL
   to the relevant personal ado plus path before test execution.
5. Prepare final pre-release checklist for dtkit with upgraded dtparquet
   (build, lock-safe DLL promotion, 1..7 batch verification, cleanup, docs).
6. Keep metadata restoration in-parquet-only (`dtparquet.dtmeta`); do not
   reintroduce sidecar metadata files.
7. Keep compression-level contract deterministic: explicit level rejects with
   `r(198)` while codec/default selection behavior remains unchanged.
8. Keep `compress_string_to_numeric` intentionally unsupported unless the
   plugin/runtime contract is explicitly redesigned and approved.

### Planned implementation sequence (next feature phase)

1. [x] Rename `rust/` to `plugin/` and update all repository references, build
   paths, and docs in one coherent patch.
2. Add deterministic global cleanup of temporary/non-relevant files (not only
   `*.tmp`) and verify no fixtures are removed.
3. Add/update release notes section listing files to track now for commit and
   files that remain intentionally untracked.
4. [x] Update each dtparquet test do-file to deploy local `dtparquet.dll` into the
   user ado plus plugin location before running assertions.
5. Run release-prep validation: `cargo build --release`, lock-safe DLL
   promotion, one-by-one `dtparquet_test1.do` through `dtparquet_test7.do`,
   then deterministic cleanup and HANDOFF refresh.

## Important Notes

- Build target dir is configured in `plugin/.cargo/config.toml` (machine-local, ignored):
  - `D:/OneDrive/tmp/plugin/dtparquet-target`
- `plugin/target/` is ignored.
- Large/untracked repo content still exists (fixtures, dll binaries, rust crate
  files, etc.).
- Keep edits minimal and scoped; do not normalize unrelated untracked files.

## Lock-safe DLL promotion flow

Use this deterministic promotion flow for `ado/ancillary_files/dtparquet.new.dll`
to `ado/ancillary_files/dtparquet.dll`:

1. Keep `dtparquet.dll` as the default plugin target.
2. Attempt direct promotion by replacing `dtparquet.dll` with
   `dtparquet.new.dll`.
3. If replacement succeeds, continue with normal batch validation.
4. If replacement fails because `dtparquet.dll` is locked, keep
   `dtparquet.dll` unchanged and temporarily run tests with
   `dtparquet.new.dll` only for that locked window.
5. After lock release, promote again and restore all plugin references back to
   `dtparquet.dll`.

This preserves a stable primary DLL path while allowing deterministic fallback
only when lock contention blocks promotion.

## Files To Read First

1. `HANDOFF.md`
2. `plugin/src/lib.rs`
3. `plugin/src/read.rs`
4. `plugin/src/write.rs`
5. `plugin/src/metadata.rs`
6. `ado/dtparquet.ado`
7. `ado/ancillary_files/test/dtparquet/dtparquet_test7.do`
8. reference: `temp_repos/stata_parquet_io-main/src/write.rs`

## Constraints

- Keep pq-compatible read contract unchanged.
- Keep existing dtparquet user syntax unchanged.
- Preserve atomic write for single-file save path.
- Prefer minimal targeted edits.
- Use Stata batch mode for validation.
- Do not switch to `dtparquet.new.dll` unless `dtparquet.dll` is locked.

## Prompt For Next Agent

Execute these tasks in one cohesive patch set.

1) Rename `plugin/` to `plugin/` and update all hardcoded references (docs,
   scripts, configs, paths) with no behavior change.

2) Clean temporary/non-relevant generated files repo-wide deterministically
   (not only `*.tmp`), excluding `temp_repos/`, and verify fixtures were not
   touched.

3) Provide a clear release-oriented file tracking decision list (track/commit
   now vs intentionally untracked) and apply only agreed changes.

4) Update all dtparquet test do-files to deploy/copy local `dtparquet.dll` to
   the user's ado plus location before test execution.

5) Prepare release state for upgraded dtparquet: build release, promote DLL
   lock-safely, run tests 1..7 in order, confirm pass/fail matrix, clean
   generated artifacts, and refresh `HANDOFF.md`.

Constraints for this agent:

- Keep command syntax unchanged.
- Keep edits minimal, localized, and reversible.
- Keep tests deterministic and clean up generated artifacts.
- Do not weaken existing assertions.
- Do not add external tooling dependencies.

Validation required:

- Run `dtparquet_test1.do` through `dtparquet_test7.do` one-by-one in batch.
- Use `dtparquet.new.dll` only when `dtparquet.dll` is locked.

Latest rerun after locking compression-level contract (explicit level rejected)
and extending `dtparquet_test7.do` compression assertions:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass. `dtparquet_test7.do` now asserts
deterministic compression behavior for `compress()` codecs/default and rejects
explicit plugin compression levels (`zstd` level `3`, `snappy` level `1`) with
`r(198)`. Deterministic cleanup was rechecked for `rust_roundtrip.parquet`,
`rust_filtered_save.parquet`, `rust_partitioned_out`, and `*.tmp` remnants.

Latest rerun after implementing foreign parquet categorical/dictionary
compatibility on read path and adding Test 6b assertions:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; Test 6b verifies deterministic numeric
value-label restoration for foreign categorical columns while metadata-backed
`dtparquet.dtmeta` restore behavior remains unchanged.

Latest rerun after adding `catmode()` foreign categorical modes and assertions
in `dtparquet_test7.do` (Tests 6c/6d/6e):

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test1.do"`
2. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test2.do"`
3. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test3.do"`
4. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test4.do"`
5. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test5.do"`
6. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test6.do"`
7. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: all seven test files pass; Test 6c verifies `catmode(raw)`, Test 6d
verifies `catmode(both)` companion id behavior, and Test 6e verifies invalid
`catmode()` is rejected with deterministic `r(198)`.

Incremental rerun after wiring `if` pushdown and adding Test 5b assertion:

1. `"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Result: pass. `dtparquet_test7.log` includes `Test 5b PASSED: if qualifier
filtering is pushed down`.
