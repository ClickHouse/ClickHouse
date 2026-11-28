-- Tags: no-random-settings, no-random-merge-tree-settings
-- The preliminary `DISTINCT` deduplicates every read stream separately, also when distinct-in-order
-- is disabled (a hash-based `DistinctTransform` per stream). `PrefetchingConcatProcessor` must not
-- collapse a single-part read-in-order into one stream below it, which would serialize the hashing.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_prefetching_concat_distinct;

CREATE TABLE t_prefetching_concat_distinct (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat_distinct FINAL;

-- Sanity check: without `DISTINCT` the same read uses `PrefetchingConcat`.
SELECT 'without_distinct';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT path FROM t_prefetching_concat_distinct
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Expect no `PrefetchingConcat` and several parallel hash-based `DistinctTransform`.
SELECT 'distinct_not_in_order';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%DistinctTransform%' AND explain LIKE '%× %') > 0 AS has_parallel_distinct
FROM (
    EXPLAIN PIPELINE SELECT DISTINCT path FROM t_prefetching_concat_distinct
    WHERE path LIKE '%file.log'
    ORDER BY path
) SETTINGS optimize_distinct_in_order = 0;

SELECT 'distinct_not_in_order_correctness';
SELECT arr = arraySort(arr) AS is_sorted, length(arr) AS n FROM (
    SELECT groupArray(path) AS arr FROM (
        SELECT DISTINCT path FROM t_prefetching_concat_distinct
        WHERE path LIKE '%file.log'
        ORDER BY path
    )
) SETTINGS optimize_distinct_in_order = 0;

DROP TABLE t_prefetching_concat_distinct;
