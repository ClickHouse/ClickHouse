-- Tags: no-random-merge-tree-settings, no-object-storage, no-fasttest
-- Checks the peak memory of the merge, which the buffers of object storage would dominate.

SET flatten_nested = 1;
SET materialize_ttl_after_modify = 0;

-- `x` is stored in every source part, so the merge never evaluates its `DEFAULT`. Reading the expired `n.d`
-- it refers to would fill a 256 byte element for each element of `n.a` in every block.
DROP TABLE IF EXISTS ttl_nested_stored_default;
CREATE TABLE ttl_nested_stored_default (id UInt64, ts DateTime, n Nested(a UInt8), x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, merge_max_block_size = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_stored_default SELECT number, '2000-01-01', range(16), 7 FROM numbers(8192);
INSERT INTO ttl_nested_stored_default SELECT number + 8192, '2000-01-01', range(16), 7 FROM numbers(8192);
ALTER TABLE ttl_nested_stored_default ADD COLUMN n.d Array(FixedString(256));
ALTER TABLE ttl_nested_stored_default MODIFY COLUMN x UInt64 DEFAULT length(n.d);
ALTER TABLE ttl_nested_stored_default MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE id % 2 = 1;

OPTIMIZE TABLE ttl_nested_stored_default FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT count(), sum(x), sum(length(n.d)) FROM ttl_nested_stored_default;
SELECT arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_stored_default' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'x');

SYSTEM FLUSH LOGS part_log;
SELECT (peak_memory_usage < 40 * 1024 * 1024 AS ok) ? ok : peak_memory_usage FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'ttl_nested_stored_default' AND event_type = 'MergeParts';

DROP TABLE ttl_nested_stored_default;

-- The same with a stored non-deterministic `MATERIALIZED` column: `n.d` expires, but `TTLTransform` does not
-- recompute `x`, as that would change the rows the TTL did not touch, so `n.d` is not read either.
DROP TABLE IF EXISTS ttl_nested_stored_nondeterministic;
CREATE TABLE ttl_nested_stored_nondeterministic (id UInt64, ts DateTime, n Nested(a UInt8), x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, merge_max_block_size = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_stored_nondeterministic SELECT number, '2000-01-01', range(16), 7 FROM numbers(8192);
INSERT INTO ttl_nested_stored_nondeterministic SELECT number + 8192, '2000-01-01', range(16), 7 FROM numbers(8192);
ALTER TABLE ttl_nested_stored_nondeterministic ADD COLUMN n.d Array(FixedString(256));
ALTER TABLE ttl_nested_stored_nondeterministic MODIFY COLUMN x UInt64 MATERIALIZED length(n.d) + rand() % 17;
ALTER TABLE ttl_nested_stored_nondeterministic MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE id % 2 = 1;

OPTIMIZE TABLE ttl_nested_stored_nondeterministic FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT count(), sum(x), sum(length(n.d)) FROM ttl_nested_stored_nondeterministic;
SELECT arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_stored_nondeterministic' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'x');

SYSTEM FLUSH LOGS part_log;
SELECT (peak_memory_usage < 40 * 1024 * 1024 AS ok) ? ok : peak_memory_usage FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'ttl_nested_stored_nondeterministic' AND event_type = 'MergeParts';

DROP TABLE ttl_nested_stored_nondeterministic;

-- The same with a column TTL that is not due: `TTLColumnAlgorithm` does not reset `x`, so its `DEFAULT` is not
-- evaluated and `n.d` is not read either. The TTL is declared before the inserts, so the parts store its bound.
DROP TABLE IF EXISTS ttl_nested_stored_future_ttl;
CREATE TABLE ttl_nested_stored_future_ttl (id UInt64, ts DateTime, n Nested(a UInt8), x UInt64 TTL ts + INTERVAL 100 YEAR)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, merge_max_block_size = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_stored_future_ttl SELECT number, '2000-01-01', range(16), 7 FROM numbers(8192);
INSERT INTO ttl_nested_stored_future_ttl SELECT number + 8192, '2000-01-01', range(16), 7 FROM numbers(8192);
ALTER TABLE ttl_nested_stored_future_ttl ADD COLUMN n.d Array(FixedString(256));
ALTER TABLE ttl_nested_stored_future_ttl MODIFY COLUMN x UInt64 DEFAULT length(n.d) TTL ts + INTERVAL 100 YEAR;

OPTIMIZE TABLE ttl_nested_stored_future_ttl FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT count(), sum(x), sum(length(n.d)) FROM ttl_nested_stored_future_ttl;
SELECT arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_stored_future_ttl' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'x');

SYSTEM FLUSH LOGS part_log;
SELECT (peak_memory_usage < 40 * 1024 * 1024 AS ok) ? ok : peak_memory_usage FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'ttl_nested_stored_future_ttl' AND event_type = 'MergeParts';

DROP TABLE ttl_nested_stored_future_ttl;
