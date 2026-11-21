-- Tags: no-random-settings, no-random-merge-tree-settings

-- Per-part `PrefetchingConcat` concatenates the streams of a part right at the read. The `SAMPLE`
-- filter is added after the streams are built, so it would run on one stream per part.
-- `PrefetchingConcat` is therefore not used for sampled reads.

DROP TABLE IF EXISTS t_prefetching_concat_sample;
CREATE TABLE t_prefetching_concat_sample (key UInt64, payload String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY (key, cityHash64(payload)) SAMPLE BY cityHash64(payload)
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_sample SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_prefetching_concat_sample FINAL;

-- Without `SAMPLE`: `PrefetchingConcat` is used.
SELECT 'no_sample';
SELECT countIf(explain LIKE '%PrefetchingConcat%') > 0 FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_sample
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- With `SAMPLE`: `PrefetchingConcat` is not used, and there are more sampling filters than the three parts.
SELECT 'sample';
SELECT countIf(explain LIKE '%PrefetchingConcat%') = 0, maxIf(toUInt32OrZero(extract(explain, 'FilterTransform × (\\d+)')), explain LIKE '%FilterTransform%') > 3 FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_sample SAMPLE 1 / 2
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count() > 0 FROM (
    SELECT key FROM t_prefetching_concat_sample SAMPLE 1 / 2
    ORDER BY key
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_sample;
