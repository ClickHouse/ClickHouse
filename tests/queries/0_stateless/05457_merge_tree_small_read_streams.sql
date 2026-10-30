-- Tags: no-parallel-replicas, no-object-storage

DROP TABLE IF EXISTS t_small_read;
CREATE TABLE t_small_read (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_small_read SELECT 0, number FROM numbers(26 * 8192);
INSERT INTO t_small_read SELECT 1, number FROM numbers(26 * 8192);
OPTIMIZE TABLE t_small_read FINAL;

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_columns_cache = 0;

SELECT count(), sum(rows), sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_small_read' AND active;

-- 52 marks in 2 parts.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read) WHERE explain ILIKE '%MergeTreeSelect%';
-- 34 marks in 2 parts.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read WHERE k < 17 * 8192) WHERE explain ILIKE '%MergeTreeSelect%';
-- 20 marks in 1 part: still one stream read in order.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read WHERE p = 0 AND k < 20 * 8192) WHERE explain ILIKE '%MergeTreeSelect%';
-- A full sort without LIMIT keeps the default streams, with LIMIT the read is spread.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT k FROM t_small_read ORDER BY k % 7) WHERE explain ILIKE '%MergeTreeSelect%';
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT k FROM t_small_read ORDER BY k % 7 LIMIT 10) WHERE explain ILIKE '%MergeTreeSelect%';
-- A window sort by partition does not merge its streams.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT k, row_number() OVER (PARTITION BY k % 3 ORDER BY k) FROM t_small_read
    SETTINGS query_plan_enable_multithreading_after_window_functions = 1) WHERE explain ILIKE '%MergeTreeSelect%';
-- Nor do the sorts of a parallel_full_sorting_merge join.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT count() FROM t_small_read AS a INNER JOIN t_small_read AS b ON a.k % 1000 = b.k % 1000
    SETTINGS join_algorithm = 'parallel_full_sorting_merge', query_plan_join_shard_by_pk_ranges = 0) WHERE explain ILIKE '%MergeTreeSelect%';

DROP TABLE t_small_read;
