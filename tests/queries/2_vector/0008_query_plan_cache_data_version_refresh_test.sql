-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the query plan cache and its entries metric are global
--
-- `query_plan_cache_data_version_check = 1` (the default).
--
-- When the table data changes between two executions of the same vector SELECT, a cache hit
-- keeps the cached QueryPlan structure and refreshes only the data-related state of its reads
-- (`prepared_parts`, mutations snapshot, storage snapshot, `AnalysisResult`). The second
-- execution must therefore return the same rows as the same query executed with
-- `vector_query_plan_cache = 0`.
--
-- Every scenario prints the cached run and then the uncached run of the identical SQL after the
-- mutation. Both blocks must be identical in the reference file, which is the assertion that
-- there is no stale-snapshot drift. The counters prove the path: one miss for the warm-up,
-- one hit for the run after the mutation, and `QueryPlanCacheDataRefresh` incremented exactly
-- once (`QueryPlanCacheDataRefreshFailures` stays 0). `QueryCacheHits` must stay 0, so the
-- result cannot come from the result cache.
--
-- The last scenario changes the table metadata (`ALTER ... ADD COLUMN`). `ALTER` invalidates
-- cached entries for the table through the normal invalidation path, so the second execution is
-- a miss and is planned again; the result stays correct.
--
-- First version of mode 2 does not write the refreshed plan back: the entry is dropped after a
-- successful refresh, so `VectorQueryPlanCacheEntries` returns to 0.

DROP TABLE IF EXISTS vpc_refresh_run_state;
DROP TABLE IF EXISTS vpc_refresh_insert;
DROP TABLE IF EXISTS vpc_refresh_delete;
DROP TABLE IF EXISTS vpc_refresh_update;
DROP TABLE IF EXISTS vpc_refresh_alter;

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
SET enable_vector_performance_test = 1;
SET query_plan_cache_data_version_check = 1;
SET mutations_sync = 1;
SET enable_lightweight_update = 1;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE vpc_refresh_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vpc_refresh_run_state SELECT now64(6);

-- I: INSERT adds a row whose vector is exactly the query vector. After the refresh the cached
-- execution must see it, exactly like the uncached execution.
CREATE TABLE vpc_refresh_insert (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vpc_refresh_insert
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vpc_refresh_insert
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== I.1 INSERT: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_insert ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_refresh_insert_assert';
SELECT '=== I.2 INSERT: entries after the warm-up ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET vector_query_plan_cache = 1;

SET log_comment = 'vpc_refresh_insert_dml';
INSERT INTO vpc_refresh_insert VALUES (1000, [10., 13, 11]);

SET vector_query_plan_cache = 0;
SELECT '=== I.3 INSERT: cache ON, run AFTER the mutation -> refreshed, must be current ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_insert ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_refresh_insert_assert';
SELECT '=== I.4 INSERT: entries after the refresh (not written back) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SELECT '=== I.5 INSERT: cache OFF, SAME query AFTER the mutation ===';
SET log_comment = 'vpc_refresh_insert_uncached';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_insert ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;

SET log_comment = 'vpc_refresh_insert_assert';
SYSTEM FLUSH LOGS;
SELECT '=== I.6 INSERT: hits, misses, refreshes, refresh failures, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryPlanCacheDataRefresh']),
    sum(ProfileEvents['QueryPlanCacheDataRefreshFailures']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_refresh_insert'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vpc_refresh_run_state);
SET log_comment = 'vpc_refresh_insert';

-- D: DELETE removes the row at distance 0. The refreshed cached execution must not return it.
CREATE TABLE vpc_refresh_delete (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vpc_refresh_delete
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vpc_refresh_delete
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== D.1 DELETE: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_delete ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_refresh_delete_dml';
DELETE FROM vpc_refresh_delete WHERE id = 10;

SET vector_query_plan_cache = 0;
SELECT '=== D.2 DELETE: cache ON, run AFTER the mutation -> refreshed, must be current ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_delete ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_refresh_delete_assert';
SELECT '=== D.3 DELETE: entries after the refresh (not written back) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SELECT '=== D.4 DELETE: cache OFF, SAME query AFTER the mutation ===';
SET log_comment = 'vpc_refresh_delete_uncached';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_delete ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;

SET log_comment = 'vpc_refresh_delete_assert';
SYSTEM FLUSH LOGS;
SELECT '=== D.5 DELETE: hits, misses, refreshes, refresh failures, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryPlanCacheDataRefresh']),
    sum(ProfileEvents['QueryPlanCacheDataRefreshFailures']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_refresh_delete'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vpc_refresh_run_state);
SET log_comment = 'vpc_refresh_delete';

-- U: lightweight UPDATE moves the row at distance 0 away from the query vector. The refreshed
-- cached execution must return the updated Top-K.
CREATE TABLE vpc_refresh_update (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO vpc_refresh_update
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vpc_refresh_update
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== U.1 UPDATE: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_update ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_refresh_update_dml';
UPDATE vpc_refresh_update SET vector = [50., 53, 51] WHERE id = 10;

SET vector_query_plan_cache = 0;
SELECT '=== U.2 UPDATE: cache ON, run AFTER the mutation -> refreshed, must be current ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_update ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_refresh_update_assert';
SELECT '=== U.3 UPDATE: entries after the refresh (not written back) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SELECT '=== U.4 UPDATE: cache OFF, SAME query AFTER the mutation ===';
SET log_comment = 'vpc_refresh_update_uncached';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_update ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;

SET log_comment = 'vpc_refresh_update_assert';
SYSTEM FLUSH LOGS;
SELECT '=== U.5 UPDATE: hits, misses, refreshes, refresh failures, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryPlanCacheDataRefresh']),
    sum(ProfileEvents['QueryPlanCacheDataRefreshFailures']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_refresh_update'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vpc_refresh_run_state);
SET log_comment = 'vpc_refresh_update';

-- A: metadata change. `ALTER` invalidates the cached entries of the table, so the second
-- execution misses and is planned again; the result stays correct.
CREATE TABLE vpc_refresh_alter (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vpc_refresh_alter
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vpc_refresh_alter
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== A.1 ALTER: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_alter';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_alter ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_refresh_alter_dml';
ALTER TABLE vpc_refresh_alter ADD COLUMN note String DEFAULT '';

SET vector_query_plan_cache = 0;
SELECT '=== A.2 ALTER: cache ON, run AFTER the metadata change -> entry invalidated, replanned ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_refresh_alter';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_alter ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SELECT '=== A.3 ALTER: cache OFF, SAME query AFTER the metadata change ===';
SET log_comment = 'vpc_refresh_alter_uncached';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vpc_refresh_alter ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;

SET log_comment = 'vpc_refresh_alter_assert';
SYSTEM FLUSH LOGS;
SELECT '=== A.4 ALTER: hits, misses, refreshes, refresh failures, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryPlanCacheDataRefresh']),
    sum(ProfileEvents['QueryPlanCacheDataRefreshFailures']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_refresh_alter'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vpc_refresh_run_state);
SET log_comment = 'vpc_refresh_alter';

-- Cleanup.
DROP TABLE IF EXISTS vpc_refresh_insert;
DROP TABLE IF EXISTS vpc_refresh_delete;
DROP TABLE IF EXISTS vpc_refresh_update;
DROP TABLE IF EXISTS vpc_refresh_alter;
DROP TABLE vpc_refresh_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
