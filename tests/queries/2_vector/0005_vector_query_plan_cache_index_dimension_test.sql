-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the vector query plan cache and its entries metric are global
--
-- Invalidation of the vector query plan cache for tables with a vector index.
-- `I06` drops and re-adds the index around the same query, first with
-- `L2Distance` and then with `cosineDistance`: plans of both metrics are
-- warmed before the second `DROP INDEX`, so the drop must clear both entries.
-- `I03` rebuilds the indexed column with a different dimension. The 8- and
-- 16-dimensional queries differ only in the width of the vector literal, so
-- both normalize to the same template and would share a plan key unless the
-- schema change invalidated the entry.
--
-- Every DDL runs in the same session with `vector_query_plan_cache = 1`,
-- because `executeQuery` invalidates only when the session executing the DDL
-- has the cache enabled. The new rows are written by a session with the cache
-- disabled, so the `INSERT` cannot be the reason an entry disappeared.
--
-- Assertions read `system.metrics` and `system.query_log`. Any SELECT executed
-- with the cache enabled counts as a plan-cache miss, even without a vector
-- literal, so assertion reads run under a `<case>_assert` comment and are not
-- counted by the case. `QueryCacheHits` must stay 0, so every hit belongs to
-- the plan cache and not to the result cache.

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
SET enable_vector_performance_test = 1;

-- Run-scoped start time: assertions only count queries logged after it.
DROP TABLE IF EXISTS vector_plan_cache_run_state;
CREATE TABLE vector_plan_cache_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vector_plan_cache_run_state SELECT now64(6);

SET log_comment = 'vpc_index_setup';
DROP TABLE IF EXISTS vector_plan_cache_index;
DROP TABLE IF EXISTS vector_plan_cache_dimension;

-- The I06 table keeps its 8-dimensional index for the whole case.
CREATE TABLE vector_plan_cache_index
(
    id UInt32,
    vector Array(Float32),
    INDEX vi vector TYPE vector_similarity('hnsw', 'L2Distance', 8, 'bf16', 16, 100) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_index
SELECT number,
    [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1),
     toFloat32(number + 2), toFloat32(number + 4), toFloat32(number + 5),
     toFloat32(number + 6), toFloat32(number + 7)]
FROM numbers(100);

-- The I03 table is rebuilt with 16 dimensions in the middle of the case.
CREATE TABLE vector_plan_cache_dimension
(
    id UInt32,
    vector Array(Float32),
    INDEX vi vector TYPE vector_similarity('hnsw', 'L2Distance', 8, 'bf16', 16, 100) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO vector_plan_cache_dimension
SELECT number,
    [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1),
     toFloat32(number + 2), toFloat32(number + 4), toFloat32(number + 5),
     toFloat32(number + 6), toFloat32(number + 7)]
FROM numbers(100);

-- I06: DROP INDEX empties the entries and the next query misses. ADD INDEX
-- empties the entry created by that query, the next query misses again, and
-- the execution after it hits, so the plan is re-cached. The same cycle is
-- then repeated after rebuilding the index with `cosineDistance`: the plans
-- of both metrics are warmed before the `DROP INDEX`, so the drop has to
-- clear both entries.
SET log_comment = 'vpc_index_i06';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SET log_comment = 'vpc_index_i06';
ALTER TABLE vector_plan_cache_index DROP INDEX vi;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after DROP INDEX', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SELECT id, round(L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SYSTEM FLUSH LOGS;
SELECT 'I06 after DROP INDEX hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after query without index', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SET log_comment = 'vpc_index_i06';
ALTER TABLE vector_plan_cache_index ADD INDEX vi vector TYPE vector_similarity('hnsw', 'L2Distance', 8, 'bf16', 16, 100) GRANULARITY 100000000;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after ADD INDEX', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SELECT id, round(L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SYSTEM FLUSH LOGS;
SELECT 'I06 after ADD INDEX hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i06';
SELECT id, round(L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SYSTEM FLUSH LOGS;
SELECT 'I06 after re-hit hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 top-5 after index rebuild', count(), min(id), max(id)
FROM (SELECT id FROM vector_plan_cache_index ORDER BY L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), id LIMIT 5);
-- Warm the `cosineDistance` template as well, so the DROP INDEX below has to
-- clear the entries of both metrics, not only of the metric of the index.
SET log_comment = 'vpc_index_i06';
SELECT id, round(cosineDistance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries with L2 and cosine plans', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SET log_comment = 'vpc_index_i06';
ALTER TABLE vector_plan_cache_index DROP INDEX vi;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after DROP INDEX for cosine', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SET log_comment = 'vpc_index_i06';
ALTER TABLE vector_plan_cache_index ADD INDEX vi vector TYPE vector_similarity('hnsw', 'cosineDistance', 8, 'bf16', 16, 100) GRANULARITY 100000000;
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries after ADD cosine index', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';
SET log_comment = 'vpc_index_i06';
SELECT id, round(cosineDistance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SYSTEM FLUSH LOGS;
SELECT 'I06 after cosine query hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i06';
SELECT id, round(cosineDistance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_index ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i06_assert';
SYSTEM FLUSH LOGS;
SELECT 'I06 after cosine re-hit hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i06'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 top-5 with cosine distance', count(), min(id), max(id)
FROM (SELECT id FROM vector_plan_cache_index ORDER BY cosineDistance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), id LIMIT 5);
SET log_comment = 'vpc_index_i06_assert';
SELECT 'I06 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i06';

-- I03: the indexed column is dropped and re-added with 16 dimensions, and the
-- index is rebuilt for the new width. The 16-dimensional query must miss and
-- return only the rows written after the rebuild.
SET log_comment = 'vpc_index_i03';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [0., 3, 1, 2, 4, 5, 6, 7]), 4) AS d FROM vector_plan_cache_dimension ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 entries after 8-dimensional query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i03';
SET log_comment = 'vpc_index_i03';
ALTER TABLE vector_plan_cache_dimension DROP INDEX vi;
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 entries after DROP INDEX', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i03';
SET log_comment = 'vpc_index_i03';
ALTER TABLE vector_plan_cache_dimension DROP COLUMN vector;
ALTER TABLE vector_plan_cache_dimension ADD COLUMN vector Array(Float32);
TRUNCATE TABLE vector_plan_cache_dimension;
ALTER TABLE vector_plan_cache_dimension ADD INDEX vi vector TYPE vector_similarity('hnsw', 'L2Distance', 16, 'bf16', 16, 100) GRANULARITY 100000000;
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 entries after dimension change', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i03';
SET log_comment = 'vpc_index_i03';
SET vector_query_plan_cache = 0;
INSERT INTO vector_plan_cache_dimension
SELECT 1000 + number,
    [toFloat32(1000 + number), toFloat32(1003 + number), toFloat32(1001 + number),
     toFloat32(1002 + number), toFloat32(1004 + number), toFloat32(1005 + number),
     toFloat32(1006 + number), toFloat32(1007 + number), toFloat32(1008 + number),
     toFloat32(1009 + number), toFloat32(1010 + number), toFloat32(1011 + number),
     toFloat32(1012 + number), toFloat32(1013 + number), toFloat32(1014 + number),
     toFloat32(1015 + number)]
FROM numbers(50);
SET vector_query_plan_cache = 1;
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 entries after INSERT', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i03';
SELECT id, round(L2Distance(vector, [1000., 1003, 1001, 1002, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015]), 4) AS d FROM vector_plan_cache_dimension ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_index_i03_assert';
SYSTEM FLUSH LOGS;
SELECT 'I03 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_index_i03'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 top-5 after dimension change', count(), min(id), max(id), countIf(id < 1000)
FROM (SELECT id FROM vector_plan_cache_dimension ORDER BY L2Distance(vector, [1000., 1003, 1001, 1002, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015]), id LIMIT 5);
SET log_comment = 'vpc_index_i03_assert';
SELECT 'I03 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_index_i03';

-- Cleanup.
SET log_comment = 'vpc_index_setup';
DROP TABLE IF EXISTS vector_plan_cache_index;
DROP TABLE IF EXISTS vector_plan_cache_dimension;
DROP TABLE vector_plan_cache_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
