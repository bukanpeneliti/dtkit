# Phase 5 research: alternatives for dtparquet

## Goal

Identify options that improve `dtparquet` performance and also reduce implementation
similarity with `pq`, especially in read/write flow, transfer strategy, and command
surface design.

## Executive findings

- The highest immediate read bottleneck is repeated materialization in lazy batches,
  where expensive work can be re-run per batch.
- The eager read path has redundant slicing that adds overhead and can complicate
  offset correctness.
- Host transfer cost is high because writes are cell-by-cell with dynamic value
  dispatch in hot loops.
- Write-side parallelism is constrained by a serialized boundary around Stata data
  access and transfer.
- String and `strL` handling are allocation-heavy and should move to pooled, chunked
  transfer.
- Metadata checks can be made cheaper by using footer metadata parsing instead of
  scanning full file bytes.
- Similarity with `pq` is now concentrated in execution flow shape and helper
  patterns, not just naming.
- The strongest differentiator already present is dtmeta support; this can be
  expanded into a full dtparquet-native architecture.

## Alternative catalog

### Low effort, high impact

- **L1. Single-pass lazy execution cursor**
  - Performance: execute scan/filter/project once, then stream batches.
  - Similarity reduction: replaces `clone + slice + collect` style batching.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/read.rs`.

- **L2. Remove eager double-slice and push projection earlier**
  - Performance: less redundant work and lower memory movement.
  - Similarity reduction: introduces a distinct dtparquet fast path.
  - Effort: S. Risk: Low.
  - Files: `plugin/src/read.rs`.

- **L3. Typed column writers instead of per-cell dynamic dispatch**
  - Performance: tighter loops and fewer conversions in transfer hot path.
  - Similarity reduction: moves away from generic cell assignment pattern.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/read.rs`.

- **L4. Persistent thread pools**
  - Performance: avoid per-batch thread pool build overhead.
  - Similarity reduction: distinct scheduler lifecycle vs pq-like behavior.
  - Effort: S. Risk: Low.
  - Files: `plugin/src/read.rs`, `plugin/src/utilities.rs`.

- **L5. Footer-based metadata key lookup**
  - Performance: skip full-file byte scans for metadata detection.
  - Similarity reduction: reinforces dtparquet-specific metadata path.
  - Effort: S. Risk: Low.
  - Files: `plugin/src/read.rs`, `plugin/src/metadata.rs`.

- **L6. Schema-only describe mode with optional profiling**
  - Performance: avoid full materialization unless explicitly requested.
  - Similarity reduction: diverges from legacy describe flow.
  - Effort: M. Risk: Low.
  - Files: `plugin/src/read.rs`, `ado/dtparquet.ado`.

### Medium effort

- **M1. Compile `if` directly to Polars `Expr`**
  - Performance: avoid SQL parser/planner overhead and keep pushdown options.
  - Similarity reduction: removes pq-lineage SQL translation approach.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/sql_from_if.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.

- **M2. Blocked transfer API at host boundary**
  - Performance: fewer FFI crossings than cell-by-cell transfer.
  - Similarity reduction: architecture becomes block-oriented, not cell-oriented.
  - Effort: L. Risk: High.
  - Files: `plugin/src/stata_interface.rs`, `plugin/src/read.rs`,
    `plugin/src/write.rs`.

- **M3. Producer/consumer write pipeline**
  - Performance: overlap Stata extraction with parquet encoding.
  - Similarity reduction: distinct staged pipeline model.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/write.rs`, `plugin/src/utilities.rs`.

- **M4. `strL` specialized chunked path**
  - Performance: fewer allocations and less copy churn on text-heavy data.
  - Similarity reduction: independent from earlier pq strL handling patterns.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/write.rs`, `plugin/src/stata_interface.rs`.

- **M5. Compact JSON schema handoff macro**
  - Performance: fewer macro round trips and less command overhead.
  - Similarity reduction: breaks macro-per-variable protocol shape.
  - Effort: M. Risk: Medium.
  - Files: `plugin/src/lib.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`,
    `ado/dtparquet.ado`.

- **M6. Runtime adaptive batch autotuning**
  - Performance: select batch size from observed row width and throughput.
  - Similarity reduction: replaces static heuristic behavior.
  - Effort: M. Risk: Low.
  - Files: `plugin/src/utilities.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.

### High effort, strategic

- **H1. Arrow C Data bridge for internal boundaries**
  - Performance: lower-copy columnar interchange and cleaner module contracts.
  - Similarity reduction: major architecture departure from pq lineage.
  - Effort: L. Risk: High.
  - Files: `plugin/src/stata_interface.rs`, `plugin/src/read.rs`,
    `plugin/src/write.rs`.

- **H2. dtparquet execution engine module**
  - Performance: explicit planner/operators avoid repeated planning and collects.
  - Similarity reduction: eliminates isomorphic module structure.
  - Effort: L. Risk: Medium.
  - Files: `plugin/src/read.rs`, `plugin/src/write.rs`, `plugin/src/lib.rs`.

- **H3. ADO command redesign around typed payloads**
  - Performance: fewer reparsing passes and fewer macro operations.
  - Similarity reduction: command flow no longer mirrors `pq` helper patterns.
  - Effort: L. Risk: Medium.
  - Files: `ado/dtparquet.ado`.

- **H4. Optional direct parquet backend (non-Polars path)**
  - Performance: custom decode/transcode path optimized for Stata targets.
  - Similarity reduction: independent algorithmic core.
  - Effort: L. Risk: High.
  - Files: `plugin/src/read.rs`, `plugin/src/write.rs`, new backend modules.

## Recommended target architecture

- Use a **Scan plan -> Execute -> Stata sink** model as the primary dataflow.
- Keep host memory writes serialized at the Stata ABI boundary, but parallelize
  metadata, decode, filter, and transcode stages before that sink.
- Preserve and expand dtmeta as a first-class differentiator for schema, labels,
  notes, and compatibility metadata.
- Prefer explicit operators (scan/filter/project/sample/sort/transfer) over ad hoc
  DataFrame materialization points.

## Phase-based implementation sequence (no timeline)

### Phase 1: immediate wins and safe divergence

- Implement L2, L4, L5, and L6 first.
- Start L3 behind a fallback switch for quick rollback.
- Add baseline instrumentation for transfer calls, collect counts, and peak memory.

### Phase 2: pipeline and protocol improvements

- Implement L1 and M6.
- Add M3 producer/consumer write staging.
- Introduce M5 compact schema handoff with backward compatibility.

### Phase 3: strategic redesign

- Implement M1 (`if` compiler) and M4 (`strL` specialized path).
- Carve out H2 execution engine boundaries.
- Begin H3 ADO redesign and evaluate feasibility of H1/H4 prototypes.

## Implementation backlog

### Backlog rules

- One ticket equals one PR.
- Every ticket must include before/after benchmark output.
- Every ticket must pass data parity checks.
- Regressions over 5% require an explicit waiver note in PR description.

### T01: Benchmark and observability baseline

- Goal: establish reproducible performance and correctness baselines.
- Scope: benchmark harness and transfer counters.
- Files: `ado/ancillary_files/test/benchmark_dtparquet_vs_pq.do`,
  `plugin/src/stata_interface.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.
- Changes: add scenario matrix runs for narrow, wide, and string-heavy
  workloads; add transfer call and batch counters; persist output in machine-
  readable format for PR diffs.
- Acceptance criteria: benchmark script runs without manual edits; baseline
  report includes median, p90, and coefficient of variation; correctness report
  includes row count, column count, and null parity.

### T02: Eager read fast-path cleanup

- Goal: remove redundant operations on eager reads.
- Scope: read-path slicing and projection order.
- Files: `plugin/src/read.rs`.
- Changes: remove duplicate slicing logic; apply projection before expensive
  transforms where possible; keep behavior parity for offset, row count, and
  selected variables.
- Acceptance criteria: no offset drift in regression tests; eager read runtime
  improves by at least 10% on baseline datasets; data parity checks pass on all
  benchmark scenarios.

### T03: Persistent thread pool lifecycle

- Goal: avoid repeated thread pool construction cost.
- Scope: global pool lifecycle and usage.
- Files: `plugin/src/utilities.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.
- Changes: introduce persistent compute and IO pools; reuse pools across batches
  and commands; add fallback behavior for constrained environments.
- Acceptance criteria: no per-batch pool creation in profiling traces;
  throughput improves by at least 5% in multi-batch scenarios; no deadlocks or
  starvation in stress runs.

### T04: Footer-first metadata lookup

- Goal: remove full-file metadata key scans.
- Scope: metadata detection path.
- Files: `plugin/src/read.rs`, `plugin/src/metadata.rs`.
- Changes: parse footer key-value metadata for dtmeta detection; keep old path
  behind a fallback switch for emergency rollback; add tests for files with and
  without metadata keys.
- Acceptance criteria: metadata check time drops on large files; dtmeta behavior
  remains unchanged for valid files; corrupt footer paths fail fast with clear
  error messages.

### T05: Typed Stata transfer writers

- Goal: remove dynamic per-cell dispatch from hot loops.
- Scope: value conversion and host assignment path.
- Files: `plugin/src/read.rs`, `plugin/src/stata_interface.rs`.
- Changes: implement typed writers for numeric, date/time, string, and `strL`;
  keep one compatibility fallback path for unsupported edge cases; add counters
  for conversion failures and fallback usage.
- Acceptance criteria: transfer CPU time decreases by at least 15% on wide
  datasets; output parity remains exact on benchmark matrix; fallback hit rate
  stays below 1% on standard workloads.

### T06: Single-pass lazy execution cursor

- Goal: avoid repeated `collect` work per batch.
- Scope: lazy read execution model.
- Files: `plugin/src/read.rs`.
- Changes: build one execution plan and stream batches with a cursor; remove
  batch-time `clone + slice + collect` pattern; preserve ordering and filtering
  semantics.
- Acceptance criteria: number of full materializations drops to one per command
  path; read throughput improves by at least 20% on large inputs; results match
  baseline for sort, sample, and filter combinations.

### T07: Adaptive batch autotuning

- Goal: tune batch size from observed workload behavior.
- Scope: adaptive sizing and heuristics.
- Files: `plugin/src/utilities.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.
- Changes: add runtime batch tuner based on row width and transfer throughput;
  respect configured ceilings and memory guardrails; emit selected batch size in
  debug metrics.
- Acceptance criteria: variance across repeated runs decreases; mixed workload
  median runtime improves by at least 10%; memory peak stays within 10% of
  baseline.

### T08: Producer-consumer write pipeline

- Goal: overlap extraction and encoding on write operations.
- Scope: write dataflow and queueing.
- Files: `plugin/src/write.rs`, `plugin/src/utilities.rs`.
- Changes: separate producer stage and parquet encode stage with a bounded queue;
  add backpressure handling and queue telemetry; keep deterministic shutdown and
  cleanup behavior.
- Acceptance criteria: write throughput improves by at least 15% on mixed
  datasets; no unbounded memory growth under stress tests; queue metrics confirm
  steady-state overlap.

### T09: `if` expression compiler

- Goal: compile `if` conditions directly to expression trees.
- Scope: replace SQL translation path for filter execution.
- Files: `plugin/src/sql_from_if.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`.
- Changes: map parser output to expression compiler; keep SQL translation as
  compatibility fallback; add parity tests for existing `if` behavior.
- Acceptance criteria: filter planning overhead decreases on `if`-heavy
  workloads; logical semantics match baseline tests; pushdown opportunities are
  preserved where supported.

### T10: `strL` chunked arena path

- Goal: reduce allocation churn in text-heavy transfers.
- Scope: string handling path for long text.
- Files: `plugin/src/write.rs`, `plugin/src/stata_interface.rs`.
- Changes: introduce a per-batch string arena; implement chunked transfer for
  long strings; add diagnostics for truncation and binary edge cases.
- Acceptance criteria: string-heavy write runtime improves by at least 20%;
  allocation count and peak memory both decrease; no data loss in long-string
  regression tests.

### T11: Compact schema handoff protocol

- Goal: reduce command-layer overhead and decouple from pq-like shape.
- Scope: schema exchange between ADO and plugin.
- Files: `plugin/src/lib.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`,
  `ado/dtparquet.ado`.
- Changes: replace macro-per-variable handoff with compact JSON payload; keep a
  backward-compatible protocol mode during migration; add protocol versioning
  guard.
- Acceptance criteria: command setup latency decreases for wide schemas;
  backward compatibility path remains functional; protocol mismatch errors are
  explicit and actionable.

### T12: Execution engine boundaries and ADO cleanup

- Goal: establish distinct dtparquet architecture.
- Scope: module boundaries and command surface cleanup.
- Files: `plugin/src/lib.rs`, `plugin/src/read.rs`, `plugin/src/write.rs`,
  `ado/dtparquet.ado`.
- Changes: introduce `ScanPlan -> Execute -> StataSink` boundaries; refactor ADO
  dispatch to typed payload entry points; remove pq-shaped helper patterns where
  equivalents exist.
- Acceptance criteria: module boundaries are documented and enforced by tests;
  end-to-end performance does not regress; similarity hotspots are reduced in
  structural review.

### Suggested merge order

- Start with T01 through T04.
- Continue with T05 through T08.
- Finish with T09 through T12.

### Definition of done

- Benchmarks included in PR with before and after output.
- Correctness checks pass on all benchmark datasets.
- Regression policy is respected.
- Fallback switch is available for non-trivial behavior changes.
- Documentation updates included for command or protocol changes.

## Benchmark protocol

### Metrics

- End-to-end command wall time for `use`, `save`, `import`, and `export`.
- Throughput as rows per second and MB per second.
- Peak RSS, average CPU utilization, and transfer API call counts.
- Correctness checks: row/column counts, null parity, and sampled value hashes.

### Dataset matrix

- Narrow numeric: very large row count, few columns.
- Wide mixed: many columns with numeric, date/time, and string types.
- String-heavy: long text and `strL` distributions.
- Partitioned hive layout: many parquet files and nested partitions.
- Schema evolution set: missing/extra columns across files.

### Run design

- Two warmup runs, then eight measured runs per scenario.
- Report median and p90, with coefficient of variation.
- Execute both hot-cache and cold-ish conditions.
- Randomize scenario order to reduce sequence bias.

### Acceptance thresholds

- Phase 1 target: read throughput +20%, write throughput +10%.
- Phase 2 target: read throughput +35%, write throughput +20%.
- Phase 3 target: read throughput +50% on core workloads, write throughput +30%
  on mixed and string-heavy workloads.
- No scenario should regress by more than 5% without explicit justification.

## External research basis

- Stata plugin SPI constraints and `SF_*` APIs.
- Arrow Rust parquet reader/writer capabilities and metadata parser improvements.
- Arrow C Data/C Stream interfaces for zero-copy-compatible boundaries.
- DataFusion pruning and pushdown strategy references.
- parquet2 and arrow2 pipeline design references.
- Embedded DuckDB parquet query architecture as an optional reference path.
