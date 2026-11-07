-- Tags: no-random-settings, no-random-merge-tree-settings

-- Per-part `PrefetchingConcat` concatenates the streams of a part right at the read, so a step
-- between the read and the merge runs on one stream per part. A filter that is not moved to
-- `PREWHERE` must keep running on every read stream, so `PrefetchingConcat` is not used then.

DROP TABLE IF EXISTS t_prefetching_concat_filter;
CREATE TABLE t_prefetching_concat_filter (key UInt64, payload String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_filter SELECT number, toJSONString(map('x', toString(number))) FROM numbers(90000);
OPTIMIZE TABLE t_prefetching_concat_filter FINAL;

-- Only renames between the read and the merge: `PrefetchingConcat` is used.
SELECT 'no_filter';
SELECT countIf(explain LIKE '%PrefetchingConcat%') > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_filter
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- The filter is evaluated in `PREWHERE`, inside every read stream: `PrefetchingConcat` is used.
SELECT 'prewhere';
SELECT countIf(explain LIKE '%PrefetchingConcat%') > 0 FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_filter
    WHERE JSONExtractString(payload, 'x') != ''
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, optimize_move_to_prewhere = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- The filter stays above the read: `PrefetchingConcat` is not used, and there are more filters than the three parts.
SELECT 'filter_above_read';
SELECT countIf(explain LIKE '%PrefetchingConcat%') = 0, maxIf(toUInt32OrZero(extract(explain, 'FilterTransform × (\\d+)')), explain LIKE '%FilterTransform%') > 3 FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_filter
    WHERE JSONExtractString(payload, 'x') != ''
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, optimize_move_to_prewhere = 0,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count() FROM (
    SELECT key FROM t_prefetching_concat_filter
    WHERE JSONExtractString(payload, 'x') != ''
    ORDER BY key
    SETTINGS max_threads = 6, optimize_read_in_order = 1, optimize_move_to_prewhere = 0,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_filter;
