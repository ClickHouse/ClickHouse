-- A group aggregated in one block and flushed by the next block's unexpired row must count in the part's TTL bounds.
DROP TABLE IF EXISTS t_ttl_group_by_flush;
CREATE TABLE t_ttl_group_by_flush (k UInt64, ts DateTime, v UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts GROUP BY k SET ts = max(ts) + INTERVAL 40 YEAR, v = sum(v)
SETTINGS max_number_of_merges_with_ttl_in_pool = 0,
         merge_max_block_size = 1, merge_max_block_size_bytes = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1,
         max_bytes_to_merge_at_min_space_in_pool = 1,
         min_bytes_for_wide_part = 0;
INSERT INTO t_ttl_group_by_flush VALUES (0, '2000-01-01 00:00:00', 1);
INSERT INTO t_ttl_group_by_flush VALUES (1, '2090-01-01 00:00:00', 1);
OPTIMIZE TABLE t_ttl_group_by_flush FINAL;
SELECT k, ts FROM t_ttl_group_by_flush ORDER BY k;
SELECT group_by_ttl_info.min, group_by_ttl_info.max
FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by_flush' AND active;
DROP TABLE t_ttl_group_by_flush;
