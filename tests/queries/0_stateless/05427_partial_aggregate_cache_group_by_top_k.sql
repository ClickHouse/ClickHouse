-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: the GROUP BY top-K heap (`enable_group_by_top_k_optimization`) skips the keys of a part
-- that are beyond the boundary derived from the rows of other parts, so the state of a part depends on the whole
-- part set. Such queries must not use the cache, otherwise the state of a part stays stale after another part is dropped.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_top_k;

CREATE TABLE test_partial_agg_cache_top_k
(
    p UInt8,
    k UInt64
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple();

SYSTEM STOP MERGES test_partial_agg_cache_top_k;

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';
SET enable_group_by_top_k_optimization = 1;
SET max_threads = 1;

INSERT INTO test_partial_agg_cache_top_k SELECT 1, number FROM numbers(1000);
INSERT INTO test_partial_agg_cache_top_k SELECT 2, number + 1 FROM numbers(1000);

SELECT '--- Both parts';
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3 SETTINGS log_comment = 'test_partial_agg_cache_top_k_first';
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3;

SELECT '--- After the first part is dropped';
ALTER TABLE test_partial_agg_cache_top_k DROP PARTITION 1;
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3;
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3 SETTINGS use_partial_aggregate_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT '--- No cache traffic with the top-K heap';
SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_top_k_first'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Without the top-K heap the cache is used';
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3 SETTINGS enable_group_by_top_k_optimization = 0;
SELECT k, count() FROM test_partial_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 3 SETTINGS enable_group_by_top_k_optimization = 0, log_comment = 'test_partial_agg_cache_top_k_no_heap';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] > 0 AS has_hits
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_top_k_no_heap'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_top_k;
