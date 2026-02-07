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

## Validated Behavior

Batch command used:

`"C:\Program Files\StataNow19\StataMP-64.exe" /e "D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"`

Passing checks in `dtparquet_test7.log`:

- setup check passes
- describe macro contract checks pass
- read materializes rows/vars
- varlist subset path passes
- in-range path passes
- allstring path passes
- save then read-back roundtrip assertions pass
- final line: `All tests completed!`

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
| `compress` save option parity | test now | Add deterministic checks for accepted values and defaults. |
| `compress_string_to_numeric` parity | defer | Keep in backlog until plugin contract is finalized. |
| Full `_dt*` metadata parity | implement now | Prioritize labels/notes/dataset label roundtrip assertions. |
<!-- markdownlint-enable MD013 -->

### Refactored runtime limitations observed in legacy suite

- `dtparquet_test1.do`
  - Test 2: full datasignature roundtrip parity fails.
  - Test 3: metadata preservation parity (labels/notes) fails.
- `dtparquet_test5.do`
  - Test 3: UTF-8 special-character value fidelity check fails.
  - Test 5: datasignature fidelity check fails.
  - Test 11: native date/time roundtrip fidelity check fails.
  - Test 5b is currently skipped because the strL signature stress path is unstable.
- `dtparquet_test6.do`
  - Test 8: string payload fidelity with empty/long string combinations fails
    (values load as empty after roundtrip).

### Immediate next tasks

1. Fix `dtparquet_test6.do` Test 8 string payload fidelity (empty + long string)
   to restore non-empty values on roundtrip.
2. Fix `dtparquet_test5.do` Test 3 UTF-8 value fidelity mismatch.
3. Fix `dtparquet_test5.do` Test 11 native date/time roundtrip mismatch.
4. Fix datasignature parity failures in `dtparquet_test1.do` Test 2 and
   `dtparquet_test5.do` Test 5.
5. Resolve metadata parity mismatch in `dtparquet_test1.do` Test 3
   (labels/notes/dataset label contract).
6. Re-run `dtparquet_test1.do` through `dtparquet_test7.do` one-by-one in batch
   and record final pass/fail per file.

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

1) Add lock-safe DLL promotion documentation that defines a deterministic
   promotion flow from `ado/ancillary_files/dtparquet.new.dll` to
   `ado/ancillary_files/dtparquet.dll`.

2) Extend
   `ado/ancillary_files/test/dtparquet/dtparquet_test7.do` with focused
   coverage for `dtparquet export` and `dtparquet import` command paths,
   including:
   - normal export/import roundtrip
   - `replace`
   - `nolabel`
   - `allstring`
   - quoted paths containing spaces

3) Harden parsing in `ado/dtparquet.ado` for `dtparquet_export` and
   `dtparquet_import` so user-facing syntax handling remains stable across
   option combinations and quoted arguments.

Constraints for this agent:

- Keep command syntax unchanged.
- Keep edits minimal, localized, and reversible.
- Keep tests deterministic and clean up generated artifacts.
- Do not weaken existing assertions.
- Do not add external tooling dependencies.

Validation required:

- Run batch regression until green.
- Use `dtparquet.new.dll` only when `dtparquet.dll` is locked.

Batch validation command:

```bash
cd "D:\OneDrive\MyWork\00personal\stata\dtkit" && \
"C:\Program Files\StataNow19\StataMP-64.exe" /e \
"D:\OneDrive\MyWork\00personal\stata\dtkit\ado\ancillary_files\test\dtparquet\dtparquet_test7.do"
```
