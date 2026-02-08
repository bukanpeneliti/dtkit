# dtparquet Rust Refactor - Handoff

## Branch

`refactor/rust-dtparquet`

## Current State (as of commit `51f8447`)

- `dtparquet use` is plugin-first and stable through batch regression.
- Ado pre-read macro contract is wired and consumed by Rust read path.
- `dtparquet save` is now plugin-first end-to-end (no Python save bridge).
- Rust save path (`rust/src/write.rs`) now:
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
  - read-path batching/parallel internals were tuned in `rust/src/read.rs`:
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

Most recent one-by-one batch run (2026-02-08) has:

- pass: `dtparquet_test1.do` to `dtparquet_test7.do`
- fail: none

## Known Gaps (Next Priority)

- Primary next objective: remove all Python runtime dependency from dtparquet.
- Active runtime touchpoints are plugin/Stata-native in `ado/dtparquet.ado`:
  - `dtparquet_export` and `dtparquet_import` no longer call Python bridges.
  - `_check_python` is a no-op for the active runtime path.
  - `_cleanup_orphaned` is Stata-frame cleanup only.
  - metadata key regression uses plugin call `has_metadata_key`.
- Legacy Python-based tests/scripts still exist in `ado/ancillary_files/test/dtparquet`
  and can be cleaned separately if no longer needed.
- Rust parity still deferred:
  - `compress` / `compress_string_to_numeric` parity behavior on save.
  - full metadata embedding/restoration parity (`_dtvars`, `_dtlabel`,
    `_dtnotes`, `_dtinfo`, value-label fidelity).
- Save path is currently full in-memory DataFrame materialization (not chunked
  streaming write).

### Parity triage (next actions)

<!-- markdownlint-disable MD013 -->
| Item | Decision | Next action |
| :--- | :--- | :--- |
| `compress` save option parity | implemented | Keep deterministic checks for accepted values/defaults and reject explicit compression levels with `r(198)`. |
| `compress_string_to_numeric` parity | defer | Keep in backlog until plugin contract is finalized. |
| Full `_dt*` metadata parity | implement now | Prioritize labels/notes/dataset label roundtrip assertions. |
<!-- markdownlint-enable MD013 -->

### Refactored runtime limitations observed in legacy suite

- All previously recorded failing capability gaps in `dtparquet_test1.do` to
  `dtparquet_test7.do` are now closed on this branch.
- `dtparquet_test5.do` includes intentional skips by test design:
  Test 5b (strL signature stress case) and legacy pyarrow-fixture-dependent
  tests (6, 7, 8, 9a, 9b, 10).

### Latest batch pass/fail matrix (2026-02-08)

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
- Code-to-text mapping follows Stata `encode` semantics (sorted text order) and is
  stable for a fixed set of category strings.
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

1. Keep metadata restoration in-parquet-only (`dtparquet.dtmeta` key); do not
   reintroduce sidecar metadata files.
2. Keep running `dtparquet_test1.do` through `dtparquet_test7.do` one-by-one in
   batch after each metadata/save-path change.
3. Keep deterministic test cleanup for generated outputs
   (`rust_roundtrip.parquet`, `rust_filtered_save.parquet`,
   `rust_partitioned_out`, and `.tmp` remnants).
4. Keep compression-level contract deterministic: any explicit plugin
   compression level is rejected (`r(198)`), while codec selection and default
   behavior remain unchanged.
5. Keep `compress_string_to_numeric` intentionally unsupported unless the
   plugin/runtime contract is explicitly redesigned and approved.
6. Keep foreign categorical compatibility coverage in
   `dtparquet_test7.do` (Tests 6b, 6f, 6g, 6h) and extend only with
   fixture-based cases when adding new foreign producers.
7. Keep `dtparquet use` loaded-row trimming deterministic when read pushdown
   yields fewer rows than requested `in` range (`n_loaded_rows` macro contract).

### Planned implementation sequence (next feature phase)

1. Port Stata-to-SQL expression translator (`sql_from_if.rs`) to convert
   Stata-style `if` predicates to the Polars-side filter expression contract.
2. Implement `if` condition pushdown on read path so filtering occurs before
   full materialization into Stata memory.
3. Tune parallelization and batching strategy (ByColumn/ByRow and related
   chunking decisions) after functional stability is locked.

## Important Notes

- Build target dir is configured in `rust/.cargo/config.toml` (machine-local, ignored):
  - `D:/OneDrive/tmp/rust/dtparquet-target`
- `rust/target/` is ignored.
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
2. `rust/src/lib.rs`
3. `rust/src/read.rs`
4. `rust/src/write.rs`
5. `rust/src/metadata.rs`
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

1) Finalize and document deterministic compression-level behavior for
   `dtparquet save, compress()` (including invalid level handling policy).

2) Add deterministic regression assertions for the chosen compression-level
   behavior while preserving current command syntax.

3) Keep `compress_string_to_numeric` intentionally unsupported and preserve
   deterministic guardrail assertions (`r(198)`).

4) Update `HANDOFF.md` pass/fail matrix and immediate next tasks to match the
   latest verified logs with no stale statements.

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
