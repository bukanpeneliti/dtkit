# Phase 4 Plan: Further Differentiation & Performance

## Context
- Phase 3 complete: Differentiation + performance optimization done
- Current similarity: ~50% (target: 40-50%)
- All 7 test suites passing (33 test cases)

---

## Recommended Tasks

### Quick Wins (Low Effort, High Impact)

#### 1. Early Schema Validation
- **What**: Sample first N rows to validate schema before full read/write
- **Why**: Fail fast instead of discovering mismatches mid-operation
- **Files**: `read.rs`, `write.rs`
- **Similarity reduction**: Minimal (UX improvement)
- **Performance impact**: Small positive (fail early)

#### 2. Adaptive Thread Pool
- **What**: Separate thread pools for I/O vs compute operations
- **Why**: Different optimal thread counts for each operation type
- **Files**: `utilities.rs`
- **Similarity reduction**: Low
- **Performance impact**: Moderate on NUMA systems

---

### Medium Effort (Moderate Scope, Good Payoff)

#### 3. Row Group Pruning Enhancement
- **What**: Pre-scan parquet metadata to build statistics index
- **Why**: Skip irrelevant row groups for selective queries
- **Files**: `read.rs`, add metadata.rs helper
- **Similarity reduction**: Medium
- **Performance impact**: 2-3x for selective queries on large files

#### 4. Partition Pruning Enhancement
- **What**: Analyze directory structure before file scanning
- **Why**: Eliminate partition subsets early for deep hive structures
- **Files**: `read.rs` (already uses walkdir)
- **Similarity reduction**: Medium
- **Performance impact**: Significant for >1000 partitions

---

### Explore If Time Permits

#### 5. Custom Thread Pool with Rayon
- **What**: Fine-grained control via `with_min_len/with_max_len`
- **Why**: Better load balancing for small batches
- **Files**: `utilities.rs`
- **Similarity reduction**: Low
- **Performance impact**: Marginal

---

## NOT Recommended (Flash + Kimi suggestions)

These were evaluated and rejected:

| Suggestion | Reason |
|------------|--------|
| Custom streaming | Polars already efficient |
| Hash-based processing | Not relevant to parquet I/O |
| Zero-copy Stata integration | C API already efficient |
| SIMD type conversions | Polars handles internally |
| Event-driven architecture | Over-engineering |
| Bump allocation | Polars manages memory well |
| Fluent API | Stata syntax, not implementation |
| CQRS separation | Already done (read.rs/write.rs) |
| Functional pipeline | Style only, no performance gain |
| Schema evolution | Niche use case |

---

## Success Criteria
1. Similarity reduction: 50% → 40%
2. Performance: 10-15% improvement on typical workloads
3. Tests: Zero regressions

---

## Dependencies
- `parquet2` (already included) - for metadata parsing
- `walkdir` (already included) - for directory analysis
