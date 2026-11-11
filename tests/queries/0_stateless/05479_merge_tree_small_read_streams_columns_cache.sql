-- Tags: no-parallel-replicas, no-object-storage

-- A small read is spread over more streams with the columns cache too, although a stripe of the
-- columns cache (256 granules of 256 rows here) is larger than each part.

DROP TABLE IF EXISTS t_small_read_columns_cache;
CREATE TABLE t_small_read_columns_cache (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi';
INSERT INTO t_small_read_columns_cache SELECT 0, number FROM numbers(100 * 256);
INSERT INTO t_small_read_columns_cache SELECT 1, number FROM numbers(100 * 256);
OPTIMIZE TABLE t_small_read_columns_cache FINAL;

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
-- `sum` must read the parts instead of being answered from the per-part statistics.
SET use_statistics_for_sum_avg_aggregation = 0;

SELECT count(), sum(rows), sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_small_read_columns_cache' AND active;

SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read_columns_cache SETTINGS use_columns_cache = 0) WHERE explain ILIKE '%MergeTreeSelect%';
SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_small_read_columns_cache SETTINGS use_columns_cache = 1) WHERE explain ILIKE '%MergeTreeSelect%';

-- The second read is served from the columns cache.
SELECT sum(k), count() FROM t_small_read_columns_cache SETTINGS use_columns_cache = 1;
SELECT sum(k), count() FROM t_small_read_columns_cache SETTINGS use_columns_cache = 1;
SELECT sum(k), count() FROM t_small_read_columns_cache WHERE k % 3 = 1 SETTINGS use_columns_cache = 1;

DROP TABLE t_small_read_columns_cache;
