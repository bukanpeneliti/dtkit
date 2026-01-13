# dtparquet Phase 1: Implementation Lessons Learned

## 1. SFI (Stata Function Interface) Nuances

### The "Self" Trap in Frame Objects

In Python's `sfi` library, many methods on `sfi.Frame` and `sfi.Data` are instance methods that require an instantiated object, even if they look like class methods.

* **Problem**: `sfi.Frame.getName()` failed with `TypeError: Frame.getName() missing 1 required positional argument: 'self'`.
* **Solution**: Use `sfi.Frame().name` or `sfi.Frame().getCWF()` to access current frame attributes.

### Global State Management

Switching frames in Python via `sfi.Frame(name).changeToCWF()` or `sfi.SFIToolkit.stata("cwf name")` changes the global active frame in Stata.

* **Lesson**: Always capture the original frame name at the start of a function and use a `try...finally` block (or a final manual switch) to return to the original frame. This prevents leaving the user stranded in a metadata frame if the script errors out.

### Data Extraction Latency

Phase 1 uses column-major extraction (`sfi.Data.get(i)`). While simple, it requires careful handling of variable types to ensure Arrow arrays are constructed correctly.

* **Lesson**: The "Feasibility gate" confirmed that `strL` is currently safest to treat as high-capacity text strings until binary-safe SFI buffers are verified.

## 2. Stata Command Interface (ADO)

### The `syntax` Validation Trap

Standard Stata `syntax [varlist] [if] [in] ...` is not just a parser; it is a validator. It checks if variables in `varlist` exist and if variables in the `if` expression exist in the current dataset.

* **Problem**: For a `use`-style command, the data hasn't been loaded yet, so validation fails immediately with `r(111)`.
* **Solution**: Use `syntax [anything] [using/] [, ...]` to capture the command line segments as strings without validation. Then, manually split `anything` into a variable list and an `if/in` clause by iterating through tokens until the keywords `if` or `in` are encountered.

### Order of Operations in Environment Setup

When testing commands that bridge Stata and Python, the sequence of path additions is critical.

* **Problem**: Stata's ADO path and Python's `sys.path` must both be aware of the new module before any command is called.
* **Lesson**: Standardize test do-files to perform environment setup (e.g., `adopath ++`, `sys.path.insert`) as the very first block. This prevents "Module not found" errors that can be hard to debug when called through a Stata wrapper.

## 3. Metadata Round-tripping

### JSON as the Metadata Bridge

Storing Stata-specific metadata (labels, notes, formats) in Parquet's `kv_metadata` as a JSON string proved highly effective.

* **Lesson**: By using `dtmeta` to flatten internal Stata attributes into standard frames, we converted a "Stata-internal" problem into a "Data serialization" problem. This keeps the core Parquet schema interoperable with other tools (like DuckDB or Spark) while preserving Stata richness.

### Value Label Re-application

Re-applying value labels from a JSON-serialized frame requires careful handling of Stata's `label define` syntax.

* **Lesson**: Using `tempname` frames to isolate label definitions before calling `label define ..., add` prevents collisions and ensures that labels are restored even if the source data is a subset (`varlist`).

## 4. Development Workflow

### repo-Style Consistency

Every project has an implied "Testing Language."

* **Lesson**: Before writing test scripts, examine existing `.do` files in the repository. Adopting the established pattern (e.g., `passed_tests` tracking, structured `di` headers, and a final summary table) makes the code more maintainable and helps identify regressions more clearly than a simple sequence of assertions.

### Diagnostics over Assumptions

Assumptions about the SFI API were the primary source of errors.

* **Lesson**: A `diag.do` file that runs `dir(sfi.Frame)` or `help(sfi.Data)` is worth an hour of documentation searching. Always verify the environment's specific SFI capabilities before writing core logic.

### Incremental Verification

Phase 1 success was built on a "Golden Rule" test do-file (`dtparquet_test1.do`).

* **Lesson**: Testing the round-trip (Save -> Clear -> Use -> Assert) for every data type early on prevented "type drift" where a byte becomes an int or a float becomes a double.
