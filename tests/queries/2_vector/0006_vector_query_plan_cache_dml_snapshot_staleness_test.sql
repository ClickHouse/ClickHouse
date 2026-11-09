-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the vector query plan cache and its entries metric are global
--
-- Stale plan snapshot after DML in the vector query plan cache.
--
-- This is a correctness issue, not the statistics/selectivity "data drift" that
-- makes a reused plan suboptimal: the cached `QueryPlan` keeps the
-- `prepared_parts` and mutations snapshot captured when it was built, so after
-- the table changes it no longer sees the current data.
--
-- Every scenario below runs the SAME vector SELECT four times on the SAME table
-- state and in the SAME session; only `vector_query_plan_cache` differs for the
-- control run:
--
--   step 1: `vector_query_plan_cache = 1`, run before the mutation. This is a
--           miss and fills the cache with the plan built over the old snapshot.
--   step 2: change the table (INSERT / DELETE / lightweight UPDATE).
--   step 3: `vector_query_plan_cache = 1`, the very same SQL text again. This
--           is a hit, so it is served by the cached plan and returns the result
--           computed over the snapshot taken in step 1.
--   step 4: `vector_query_plan_cache = 0`, the very same SQL text again. No
--           plan is restored, so it returns the result that matches the table
--           contents after the mutation.
--
-- Steps 3 and 4 print their rows one after the other under explicit labels, so
-- the stale result is visible as a plain diff of two result sets produced by
-- the same query. The `log_comment` is the same for both runs (it is part of
-- the cache key, and the control run is executed with caching disabled anyway),
-- therefore the only difference between the two executions is the setting
-- under test.
--
-- The separator `SELECT '...'` statements run with `vector_query_plan_cache = 0`
-- so they can never add an entry and the `VectorQueryPlanCacheEntries` assertions
-- stay exact: the entry survives every mutation and is still 1 after the cached
-- run. `VectorQueryPlanCacheHits` is 1 together with 1 miss, which proves that
-- step 3 reused the entry instead of planning again. `QueryCacheHits` stays 0,
-- so the results cannot come from the query result cache.

DROP TABLE IF EXISTS vector_plan_cache_drift_run_state;
DROP TABLE IF EXISTS vector_plan_cache_drift_insert;
DROP TABLE IF EXISTS vector_plan_cache_drift_delete;
DROP TABLE IF EXISTS vector_plan_cache_drift_update;

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
-- Data version checking must be disabled here: it is the only behavior that can return the stale snapshot.
SET query_plan_cache_data_version_check = 0;
SET enable_vector_performance_test = 1;
SET mutations_sync = 1;
SET enable_lightweight_update = 1;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE vector_plan_cache_drift_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vector_plan_cache_drift_run_state SELECT now64(6);

-- I: INSERT adds a row whose vector is exactly the query vector. The cached
-- execution keeps the old Top-K and misses the new row, while the uncached
-- execution returns the new row at distance 0.
CREATE TABLE vector_plan_cache_drift_insert (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_drift_insert
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_drift_insert
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== I.1 INSERT: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_insert ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_insert_dml';
INSERT INTO vector_plan_cache_drift_insert VALUES (1000, [10., 13, 11]);
SET log_comment = 'vpc_drift_insert_assert';
SELECT '=== I.2 INSERT: entry survived the mutation (entries) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET vector_query_plan_cache = 0;
SELECT '=== I.3 INSERT: cache ON, run AFTER the mutation -> stale result ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_insert ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_insert_assert';
SELECT '=== I.4 INSERT: entries after the cached run ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET log_comment = 'vpc_drift_insert';
SELECT '=== I.5 INSERT: cache OFF, SAME query AFTER the mutation -> current result ===';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_insert ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_drift_insert_assert';
SYSTEM FLUSH LOGS;
SELECT '=== I.6 INSERT: cache hits, misses, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_drift_insert'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_drift_run_state);
SET vector_query_plan_cache = 1;

-- D: DELETE removes the row at distance 0. The cached execution still returns
-- it, while the uncached execution returns the deletion-aware Top-K.
CREATE TABLE vector_plan_cache_drift_delete (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_drift_delete
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_drift_delete
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== D.1 DELETE: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_delete ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_delete_dml';
DELETE FROM vector_plan_cache_drift_delete WHERE id = 10;
SET log_comment = 'vpc_drift_delete_assert';
SELECT '=== D.2 DELETE: entry survived the mutation (entries) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET vector_query_plan_cache = 0;
SELECT '=== D.3 DELETE: cache ON, run AFTER the mutation -> stale result ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_delete ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_delete_assert';
SELECT '=== D.4 DELETE: entries after the cached run ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET log_comment = 'vpc_drift_delete';
SELECT '=== D.5 DELETE: cache OFF, SAME query AFTER the mutation -> current result ===';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_delete ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_drift_delete_assert';
SYSTEM FLUSH LOGS;
SELECT '=== D.6 DELETE: cache hits, misses, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_drift_delete'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_drift_run_state);
SET vector_query_plan_cache = 1;

-- U: lightweight UPDATE moves the row at distance 0 away from the query vector.
-- The cached execution still returns it at distance 0, while the uncached
-- execution returns the updated Top-K.
CREATE TABLE vector_plan_cache_drift_update (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO vector_plan_cache_drift_update
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_drift_update
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;

SET vector_query_plan_cache = 0;
SELECT '=== U.1 UPDATE: cache ON, run BEFORE the mutation (fills the cache) ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_update ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_update_dml';
UPDATE vector_plan_cache_drift_update SET vector = [50., 53, 51] WHERE id = 10;
SET log_comment = 'vpc_drift_update_assert';
SELECT '=== U.2 UPDATE: entry survived the mutation (entries) ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET vector_query_plan_cache = 0;
SELECT '=== U.3 UPDATE: cache ON, run AFTER the mutation -> stale result ===';
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_drift_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_update ORDER BY d, id LIMIT 5;

SET vector_query_plan_cache = 0;
SET log_comment = 'vpc_drift_update_assert';
SELECT '=== U.4 UPDATE: entries after the cached run ===', value
FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

SET log_comment = 'vpc_drift_update';
SELECT '=== U.5 UPDATE: cache OFF, SAME query AFTER the mutation -> current result ===';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_drift_update ORDER BY d, id LIMIT 5;

SET log_comment = 'vpc_drift_update_assert';
SYSTEM FLUSH LOGS;
SELECT '=== U.6 UPDATE: cache hits, misses, result cache hits ===',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_drift_update'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_drift_run_state);
SET vector_query_plan_cache = 1;

-- Cleanup.
DROP TABLE IF EXISTS vector_plan_cache_drift_insert;
DROP TABLE IF EXISTS vector_plan_cache_drift_delete;
DROP TABLE IF EXISTS vector_plan_cache_drift_update;
DROP TABLE vector_plan_cache_drift_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
