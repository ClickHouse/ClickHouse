-- Tags: no-parallel-replicas, no-object-storage

-- A small read is spread over all requested streams with the columns cache enabled too:
-- the read pool does not align the tasks to stripes of the cache larger than the share of a stream.

DROP TABLE IF EXISTS t_small_read_cc;
CREATE TABLE t_small_read_cc (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_small_read_cc SELECT 0, number FROM numbers(26 * 8192);
INSERT INTO t_small_read_cc SELECT 1, number FROM numbers(26 * 8192);
OPTIMIZE TABLE t_small_read_cc FINAL;

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_columns_cache = 1;
SET use_statistics_for_sum_avg_aggregation = 0;

-- 52 marks in 2 parts.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read_cc) WHERE explain ILIKE '%MergeTreeSelect%';
-- 34 marks in 2 parts.
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read_cc WHERE k < 17 * 8192) WHERE explain ILIKE '%MergeTreeSelect%';

-- The result does not depend on how the read is cut, cold and warm.
SELECT sum(k), count() FROM t_small_read_cc WHERE k < 17 * 8192;
SELECT sum(k), count() FROM t_small_read_cc WHERE k < 17 * 8192;
SELECT sum(k), count() FROM t_small_read_cc;

DROP TABLE t_small_read_cc;
