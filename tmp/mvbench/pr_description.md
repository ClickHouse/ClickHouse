<!--
Related: https://github.com/ClickHouse/ClickHouse/pull/50697
Related: https://github.com/ClickHouse/ClickHouse/pull/46558
-->

Ingestion into a `MergeTree` table with a dependent materialized view degrades roughly linearly with the fraction of inserted rows matched by the view, and at 100% match the throughput is cut roughly in half. Profiling the insert shows that the dominant part of that overhead is not the second write itself: the view target's sink re-sorts the matched rows by its primary key from scratch (`stableGetPermutation` + column permute in `MergeTreeDataWriter::writeTempPart`), repeating the sorting work the destination table's sink already performed on the very same rows. In a benchmark with 1M-row inserts of data not sorted by the primary key (8 concurrent clients, `SELECT *` view with a matching sort key), the redundant sort and permute account for ~77% of the extra CPU; the write and compression only ~23%.

This PR adds a setting `presort_inserts_with_materialized_views` (default off). When enabled and the destination table is a `MergeTree` table with a sorting key and dependent materialized views, each block is sorted by the destination table's sorting key once, before the destination sink. The destination sink then takes the existing `isAlreadySorted` fast path (#50697), and order-preserving view queries (filters, projections) deliver the rows to their target sinks already sorted, so those sinks skip sorting too when their sorting key is a prefix of the destination's. In the benchmark above, the CPU overhead of a 100%-match 1:1 materialized view drops from +73% to +29% per insert.

Views targeting an engine that resolves rows with equal sorting keys positionally — `CollapsingMergeTree`, `CoalescingMergeTree`, `GraphiteMergeTree`, or `ReplacingMergeTree` without a version column — must observe the rows in the original insertion order. Instead of disabling the presort for the whole insert, the presort saves the inverse permutation on the chunk (8 bytes per row, shared between the per-view branches; deliberately not a second copy of the columns, which would keep 2x of every in-flight block alive), and the branches of such views restore the original order with a single column permute before their view query runs. Order-insensitive views on the same source still benefit. The presort is skipped when:
- some element of the destination sorting key is an expression rather than a plain column;
- every dependent view requires the original row order (nothing would benefit);
- insert deduplication is enabled and the block carries more than one deduplication token: the tokens map to ranges of row offsets, and reordering rows would break self-deduplication. Asynchronous inserts produce one token per buffered mini-insert, so their flushed blocks are presorted only when `async_insert_deduplicate` is disabled (the default).

The destination table itself always produces byte-identical parts: its sink sorts by the same key with a stable permutation either way, and the presort is a stable sort as well. Remaining caveats (documented in the setting description): view queries observe rows in sorting-key order rather than insertion order, which matters for order-dependent constructs (`LIMIT` without `ORDER BY`, `any`, `groupArray`, ties in `argMax`, window functions without `ORDER BY`), and toggling the setting between retries of the same insert changes content-derived deduplication tokens in dependent views.

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Added a setting `presort_inserts_with_materialized_views` (default off) that sorts inserted blocks by the destination table's sorting key before the sink when the destination `MergeTree` table has dependent materialized views. The destination sink and the sinks of order-compatible materialized view targets then skip their own sorting step, removing a redundant re-sort of the rows matched by a view, which could otherwise cut ingestion throughput roughly in half. Views whose results depend on the positional order of rows (`CollapsingMergeTree`, `CoalescingMergeTree`, `GraphiteMergeTree`, or versionless `ReplacingMergeTree` targets) observe the rows restored to the original insertion order and are not affected.

### Documentation entry for user-facing changes

- [x] Documentation is written (the setting description in `Settings.cpp` is the source for the generated settings documentation)
