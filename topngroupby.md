## TopNAggregation: Fused operator for `GROUP BY ... ORDER BY aggregate LIMIT K`

### Problem

Queries of the form:

```sql
SELECT trace_id, max(start_time)
FROM traces
GROUP BY trace_id
ORDER BY max(start_time) DESC
LIMIT 10
```

currently execute as three separate steps: full aggregation over all rows, full sort of all groups, then take K. This reads the entire table and uses O(unique_groups) memory, even when only K results are needed.

See https://github.com/ClickHouse/ClickHouse/issues/75098

### Use cases

**Observability / tracing.** "Show the 10 most recently active traces":
```sql
SELECT trace_id, max(start_time) FROM traces GROUP BY trace_id ORDER BY max(start_time) DESC LIMIT 10
```
Table has billions of spans, millions of unique trace IDs, but only 10 are needed.

**Monitoring.** "Top 10 most recently active hosts":
```sql
SELECT host, max(time) FROM metrics GROUP BY host ORDER BY max(time) DESC LIMIT 10
```

**Multi-aggregate with argMax.** "Top 10 traces with their service name":
```sql
SELECT trace_id, argMax(service_name, start_time), max(start_time)
FROM traces GROUP BY trace_id ORDER BY max(start_time) DESC LIMIT 10
```
All aggregates (`argMax` and `max`) are determined by first row when reading `start_time` DESC.

**Non-example: incompatible aggregates.** `count(*)`, `sum`, `avg` and similar aggregates need all rows, so the optimization does not apply when any such aggregate is present.

### Solution: TopNAggregation operator

Replace the `AggregatingStep → SortingStep → LimitStep` chain with a single `TopNAggregatingStep` that fuses aggregation, ordering, and limiting. Two modes depending on table sort order:

#### Mode 1 — Early termination (sorted input)

When the table sorting key aligns with the ORDER BY aggregate's argument (e.g., table `ORDER BY start_time`, query `ORDER BY max(start_time) DESC`), read in physical order. Each group's aggregate is determined by its first occurrence. Stop after K groups.

```
TopNAggregating (stops after K groups)
  MergingSortedTransform N → 1  (merges N sorted reading threads)
    ReadFromMergeTree × N  (in-order read)
```

Reads O(K) rows. Memory: O(K).

#### Mode 2 — Parallel aggregation with threshold pruning (unsorted input)

When the table is not sorted by the aggregate argument, use N parallel workers that each aggregate all their rows, then emit only their local top K groups. A lightweight merge combines the partial results.

```
TopNAggregatingMerge (merges ≤ N×K entries → final top K)
  Resize N → 1
    TopNAggregating × N (emits local top K as intermediate states)
      ReadFromMergeTree × N
```

Two layers of pruning reduce work:

1. **In-transform threshold** (`topn_aggregation_pruning_level >= 1`): After the first chunk, compute the K-th best aggregate value across all groups seen so far. Skip subsequent rows whose raw argument is below this threshold — no hash lookup, no aggregate update.

2. **Dynamic filter pushdown** (`topn_aggregation_pruning_level >= 2`, requires `use_top_k_dynamic_filtering = 1`): Publish the threshold to a shared `TopKThresholdTracker`. A `__topKFilter` PREWHERE injected into `ReadFromMergeTree` uses it to skip entire granules at the storage layer.

The threshold is monotonic (only tightens) and derived from group-level aggregates, so it never over-prunes.

**Parallel scaling:** Each worker emits at most K groups, so the merge processes at most N×K entries instead of N×total_groups. This keeps the merge cheap and lets per-thread pruning gains scale linearly.

**Preconditions:** Mode 2 requires numeric non-nullable argument, no existing PREWHERE, `output_ordered_by_sort_key = true` for the ORDER BY aggregate, and `LIMIT <= topn_aggregation_max_limit` (default 1000).

### Supported aggregate functions

All aggregates in the query must be "determined by first row" in a consistent direction:

| Function | Direction | Can be ORDER BY aggregate? | Notes |
|---|---|---|---|
| `max(col)` | DESC | Yes | First row has max when reading DESC |
| `min(col)` | ASC | Yes | First row has min when reading ASC |
| `any(col)` | any | No | Arbitrary value; works in any direction |
| `argMax(val, col)` | DESC | No | Returns `val` from max-`col` row |
| `argMin(val, col)` | ASC | No | Returns `val` from min-`col` row |

"Can be ORDER BY aggregate" means the function's result is monotonic with the sort key (`output_ordered_by_sort_key = true`), needed for Mode 1 early termination and Mode 2 threshold estimation. `argMax`/`argMin` work as companion aggregates alongside `max`/`min`.

Functions that do NOT qualify: `count`, `sum`, `avg`, `uniq`, `quantile`, `groupArray`, conditional variants like `maxIf`.

### Query plan optimization

`optimizeTopNAggregation` matches this plan pattern:

```
AggregatingStep → [ExpressionStep] → SortingStep → LimitStep
```

Checks: ORDER BY references a single aggregate column, that aggregate supports top-K, all aggregates agree on direction, no HAVING/TOTALS/WITH TIES/grouping sets/OFFSET.

Mode selection:
- **Mode 1** if `output_ordered_by_sort_key` and the table's first sorting column matches the aggregate argument.
- **Mode 2** if Mode 1 doesn't apply but preconditions are met (MergeTree, numeric non-nullable argument, `pruning_level >= 1`, `LIMIT <= max_limit`).
- **Neither** otherwise; falls back to the standard pipeline.

### Settings

| Setting | Default | Description |
|---|---|---|
| `optimize_topn_aggregation` | 0 | Enable/disable the optimization |
| `topn_aggregation_pruning_level` | 2 | Mode 2 pruning: 1 = in-transform threshold, 2 = + dynamic filter (requires `use_top_k_dynamic_filtering`) |
| `topn_aggregation_max_limit` | 1000 | Maximum K for Mode 2 activation |

### Implementation files

| File | Purpose |
|---|---|
| `IAggregateFunction.h` | `getTopKAggregateInfo` virtual method |
| `AggregateFunctionsMinMax.cpp` | `max` / `min` overrides |
| `AggregateFunctionAny.cpp` | `any` override |
| `AggregateFunctionArgMinMax.cpp` | `argMax` / `argMin` overrides |
| `TopNAggregatingTransform.h/.cpp` | Mode 1 + Mode 2 transforms |
| `TopNAggregatingStep.h/.cpp` | Query plan step; builds pipeline |
| `optimizeTopNAggregation.cpp` | Plan rewrite rule |
| `TopKThresholdTracker.h` | Shared threshold with version counter |
| `FunctionTopKFilter.cpp` | `__topKFilter` PREWHERE function |

### Limitations

- **Only min/max-family aggregates.** Queries mixing `sum`/`count`/`avg` cannot be optimized.
- **Mode 1 requires sort key match.** The aggregate argument must be the first column of the MergeTree sorting key. Composite keys like `ORDER BY (service, name, start_time)` don't match for `max(start_time)`.
- **Mode 2 is tuned for small K.** Large LIMIT values are gated by `topn_aggregation_max_limit`.
- **HAVING / TOTALS / grouping sets / FINAL** are not supported.
- **Non-MergeTree engines** are not supported.
- **Distributed queries** target local execution only (distributed support is a follow-up).
- **ORDER BY on expressions** like `ORDER BY max(col) + 1` is not supported.

### Design tradeoffs

**Serialized-key HashMap vs Aggregator's type-dispatched hash tables.** Mode 2 uses a generic `HashMapWithSavedHash<std::string_view, size_t>` that serializes all group key columns into Arena-backed byte strings. The standard `Aggregator` instead dispatches at compile time to specialized hash tables for common key types (single UInt64, fixed-size keys, etc.), which avoids serialization overhead and yields better cache behavior.

Benchmarks confirm the type-dispatched tables are faster for raw aggregation: Mode 2 at pruning level 0 (direct compute only, no threshold) is ~2x slower than the baseline `Aggregator` pipeline. Mode 2's net benefit comes entirely from threshold pruning (level 1+), which skips most rows from hashing and aggregation. The serialized-key approach was chosen because:

- It is key-type-agnostic, handling single columns, composite keys, and variable-length strings uniformly without adding a type-dispatch layer to `TopNAggregatingTransform`.
- The `Aggregator` API does not expose a way to iterate groups, extract per-group aggregate values, or selectively destroy states — all needed for threshold computation and partial top-K emission. Extending that API would require nontrivial changes to a critical, heavily-optimized component.
- The performance gap only matters when threshold pruning is ineffective (very large K or perfectly uniform data), and the optimizer gates Mode 2 on `pruning_level >= 1` to avoid activating it without pruning.

A future improvement could delegate the hash table to the `Aggregator` infrastructure while keeping the pruning/emission logic in the fused operator, combining the best of both approaches.

**Threshold refresh cost.** `refreshThresholdFromStates` materializes the ORDER BY aggregate from all group states and partial-sorts to find the K-th value. This is O(groups) per refresh, which is expensive when groups are numerous. The current implementation mitigates this with two mechanisms: (1) a hard upper-bound gate that disables refresh when groups exceed K×10000, and (2) an adaptive backoff that reduces refresh frequency as more chunks arrive (every chunk initially, down to every 16–32 chunks after 2048+ chunks), with an additional ratio-based throttle when `groups/K >= 100`. Despite these heuristics, high-cardinality workloads near the K×10000 boundary can still incur significant refresh cost. Sampling a subset of groups or maintaining an incremental structure (e.g., an updateable top-K heap) could further reduce this overhead.

**Dynamic filter effectiveness depends on data distribution.** The `__topKFilter` PREWHERE is highly effective for skewed data (up to 97% I/O reduction in benchmarks) because the threshold converges quickly to a selective value. For uniform distributions, the filter provides modest benefit (~17% latency improvement) because most values are near the threshold. The filter also requires scanning the filter column for every granule even when no rows pass, contributing a fixed overhead. This is why it is gated behind both `topn_aggregation_pruning_level >= 2` and `use_top_k_dynamic_filtering = 1`.

**Partial top-K correctness and its cost.** Each parallel worker emits its local top K groups as intermediate aggregate state columns, so the merge sees at most N×K entries. This is correct because: if a group belongs to the global top K, the partial containing its globally-best aggregate value cannot rank it below K locally. The tradeoff is that the merge must re-hash and merge aggregate states (via `IAggregateFunction::merge`) for groups that appear in multiple partials. For small K this is negligible; for large K it could become significant.

### Future improvements

- **Type-dispatched hash table.** Delegate group state management to the `Aggregator`'s specialized hash tables while keeping the fused operator's pruning, partial top-K emission, and pipeline fusion. This would close the level-0 performance gap and make Mode 2 competitive even without threshold pruning.
- **In-stream group eviction.** Once enough groups are seen (e.g., 10×K), evict groups that are clearly outside the top K. This would bound Mode 2 memory to O(K) instead of O(unique_groups). Requires careful correctness reasoning when groups reappear after eviction.
- **Mixed-aggregate support (two-pass).** First pass identifies the top K groups using only the ORDER BY aggregate. Second pass re-scans the relevant data to compute remaining aggregates (`count`, `sum`, etc.) for those K groups only.
- **Distributed query support.** Each shard runs `TopNAggregation` locally and returns K groups. The initiator merges shard results (at most `num_shards × K` entries) with the same merge transform.
- **Monotonic expression support.** Allow `ORDER BY f(max(col))` when `f` is monotonic (e.g., `ORDER BY -max(col)`, `ORDER BY max(col) + 1`).
- **Nullable / string threshold support.** Relax the numeric non-nullable gate for Mode 2 by adding NULL-ordering and collation-aware threshold comparisons to `TopKThresholdTracker` and `__topKFilter`.
- **Incremental threshold maintenance.** Replace the current `refreshThresholdFromStates` (full materialization + partial sort) with an incremental top-K heap that updates as groups are inserted, eliminating the O(groups) refresh cost.

### Alternatives considered

**A. Push threshold through existing aggregation.** The current `optimizeTopK` uses `TopKThresholdTracker` for `ORDER BY col LIMIT K` (no GROUP BY). Extending it across `AggregatingTransform` is impractical because aggregation is blocking — the downstream sort cannot establish a threshold until all input is consumed.

**B. Add threshold side-channel to `AggregatingTransform`.** Maintains a top-K heap internally. Conflates responsibilities inside an already complex operator and cannot eliminate the downstream sort. The fused operator is cleaner.

**C. Rely on aggregation-in-order.** Only applies when GROUP BY keys prefix the sorting key. The target use case has GROUP BY on a non-key column (e.g., `GROUP BY trace_id` on `ORDER BY start_time`).
