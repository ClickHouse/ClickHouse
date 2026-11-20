-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the vector query plan cache and its entries metric are global
--
-- `query_plan_cache_data_version_check = 1` (the default).
--
-- The same scenarios as 0006, but the cached plan is validated against the data
-- version of the storage snapshot it was built from. When an INSERT, DELETE or
-- lightweight UPDATE changes the table, a cache hit first refreshes the data
-- snapshot of the cached plan and then executes it; the cached execution must
-- therefore return exactly the same result as the uncached execution, unlike
-- 0006 where it returns the old Top-K.
--
-- A DML does not drop entries that recorded data versions, so the entries
-- metric stays 1 after the mutation. The hit is then consumed by the refresh
-- path, which does not write the refreshed plan back, so the entries metric
-- returns to 0 at the end.

DROP TABLE IF EXISTS vector_plan_cache_version_run_state;
DROP TABLE IF EXISTS vector_plan_cache_version_insert;
DROP TABLE IF EXISTS vector_plan_cache_version_delete;
DROP TABLE IF EXISTS vector_plan_cache_version_update;

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
SET query_plan_cache_data_version_check = 1;
SET enable_vector_performance_test = 1;
SET mutations_sync = 1;
SET enable_lightweight_update = 1;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE vector_plan_cache_version_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vector_plan_cache_version_run_state SELECT now64(6);

-- I: INSERT adds a row at distance 0. The version check rejects the old plan.
CREATE TABLE vector_plan_cache_version_insert (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_version_insert
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_version_insert
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SET log_comment = 'vpc_version_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_insert ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_insert_dml';
INSERT INTO vector_plan_cache_version_insert VALUES (1000, [10., 13, 11]);
SET log_comment = 'vpc_version_insert_assert';
SELECT 'insert: entries after INSERT', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_insert';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_insert ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_insert_assert';
SELECT 'insert: entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_insert_uncached';
SET vector_query_plan_cache = 0;
SELECT 'insert uncached after mutation', id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_insert ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_version_insert_assert';
SYSTEM FLUSH LOGS;
SELECT 'insert: hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_version_insert'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_version_run_state);
SET log_comment = 'vpc_version_insert';

-- D: DELETE removes the row at distance 0. The version check rejects the old plan.
CREATE TABLE vector_plan_cache_version_delete (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_version_delete
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_version_delete
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SET log_comment = 'vpc_version_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_delete ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_delete_dml';
DELETE FROM vector_plan_cache_version_delete WHERE id = 10;
SET log_comment = 'vpc_version_delete_assert';
SELECT 'delete: entries after DELETE', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_delete';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_delete ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_delete_assert';
SELECT 'delete: entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_delete_uncached';
SET vector_query_plan_cache = 0;
SELECT 'delete uncached after mutation', id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_delete ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_version_delete_assert';
SYSTEM FLUSH LOGS;
SELECT 'delete: hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_version_delete'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_version_run_state);
SET log_comment = 'vpc_version_delete';

-- U: lightweight UPDATE moves the row at distance 0 away. The version check rejects the old plan.
CREATE TABLE vector_plan_cache_version_update (id UInt32, vector Array(Float32))
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO vector_plan_cache_version_update
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
ALTER TABLE vector_plan_cache_version_update
ADD INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SET log_comment = 'vpc_version_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_update ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_update_dml';
UPDATE vector_plan_cache_version_update SET vector = [50., 53, 51] WHERE id = 10;
SET log_comment = 'vpc_version_update_assert';
SELECT 'update: entries after UPDATE', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_update';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_update ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_version_update_assert';
SELECT 'update: entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_version_update_uncached';
SET vector_query_plan_cache = 0;
SELECT 'update uncached after mutation', id, round(L2Distance(vector, [10., 13, 11]), 4) AS d
FROM vector_plan_cache_version_update ORDER BY d, id LIMIT 5;
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_version_update_assert';
SYSTEM FLUSH LOGS;
SELECT 'update: hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_version_update'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_version_run_state);
SET log_comment = 'vpc_version_update';

-- Cleanup.
DROP TABLE IF EXISTS vector_plan_cache_version_insert;
DROP TABLE IF EXISTS vector_plan_cache_version_delete;
DROP TABLE IF EXISTS vector_plan_cache_version_update;
DROP TABLE vector_plan_cache_version_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
