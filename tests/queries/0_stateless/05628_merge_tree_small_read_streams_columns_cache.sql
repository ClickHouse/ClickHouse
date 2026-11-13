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

-- Small granules: a stripe of the cache is 256 granules, more than the share of a thread (301 / 4 marks).
-- With tasks of one granule (`merge_tree_min_rows_for_concurrent_read` = 64) and blocks limited only by the tasks,
-- the size of a block is the size of a task. If the tasks were aligned to the stripes, the first thread would take
-- a whole stripe of 16384 rows, and the others nothing.
DROP TABLE IF EXISTS t_small_granules_cc;
CREATE TABLE t_small_granules_cc (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
INSERT INTO t_small_granules_cc SELECT number FROM numbers(300 * 64);
OPTIMIZE TABLE t_small_granules_cc FINAL;

SET merge_tree_min_rows_for_concurrent_read = 64, merge_tree_min_bytes_for_concurrent_read = 1, merge_tree_min_read_task_size = 1;
SET max_block_size = 1000000, preferred_block_size_bytes = 1000000000000;
SET allow_prefetched_read_pool_for_local_filesystem = 0, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;

SELECT sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_small_granules_cc' AND active;
SELECT 'without the cache', max(c), count(), sum(k) FROM (SELECT blockSize() AS c, k FROM t_small_granules_cc) SETTINGS use_columns_cache = 0;
SELECT 'cold', max(c), count(), sum(k) FROM (SELECT blockSize() AS c, k FROM t_small_granules_cc) SETTINGS use_columns_cache = 1;
SELECT 'warm', max(c), count(), sum(k) FROM (SELECT blockSize() AS c, k FROM t_small_granules_cc) SETTINGS use_columns_cache = 1;

DROP TABLE t_small_granules_cc;
