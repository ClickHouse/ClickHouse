-- A vertical merge under a `GROUP BY` TTL that is not due keeps every row when its TTL step fills `c`, a merging column (via the two-column index) that no part stores.
-- Red if the merge fails or loses rows; the part_log check pins that the merge ran Vertical.
-- Part layout and vertical-merge thresholds are pinned in SETTINGS, so no random-settings tag.

SET optimize_throw_if_noop = 1;
CREATE TABLE t1 (k UInt64, d DateTime, c String TTL d + INTERVAL 1 DAY, v UInt64, INDEX idx_ck (c, k) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY (k, d) TTL d + INTERVAL 100 YEAR GROUP BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;
CREATE TABLE t2 AS t1;
INSERT INTO t1 SELECT number, toDateTime('2000-01-01 00:00:00') + number, 'c', number FROM numbers(100);
OPTIMIZE TABLE t1 FINAL;
INSERT INTO t2 SELECT number + 100, toDateTime('2000-01-01 00:00:00') + number, 'c', number FROM numbers(100);
OPTIMIZE TABLE t2 FINAL;
SELECT 'parts storing c', countIf(column = 'c') FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t1', 't2') AND active;
ALTER TABLE t1 ATTACH PARTITION tuple() FROM t2;
OPTIMIZE TABLE t1 FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT 'merge', merge_algorithm, rows FROM system.part_log
WHERE database = currentDatabase() AND table = 't1' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't1' AND active);
SELECT 'rows', count(), sum(v) FROM t1;
