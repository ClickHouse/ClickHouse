-- Tags: no-parallel, no-fasttest
-- no-fasttest: vector search needs the usearch 3rd party library
-- no-parallel: the vector query plan cache and its entries metric are global
--
-- Invalidation of the vector query plan cache by DDL and DML. Each case runs
-- one normalized template twice with the same FORMAT and the same
-- `log_comment`, so the second execution would hit unless the statement
-- executed in between emptied the entry. `I01` warms the same literal under
-- `L2Distance` and `cosineDistance` and checks that one `ALTER` clears both
-- entries, because invalidation is keyed on the table, not on the metric.
-- The statements touch the queried table, except in `I10` and `I14`, which
-- are the negative controls: DDL on an unrelated table or in an unrelated
-- database must keep the entry and the following hit.
--
-- Every DDL runs in the same session with `vector_query_plan_cache = 1`,
-- because `executeQuery` invalidates only when the session executing the DDL
-- has the cache enabled. A DDL in a session with the setting disabled leaves
-- stale entries; that known defect is not asserted here.
--
-- Assertions read `system.metrics` and `system.query_log`. Any SELECT executed
-- with the cache enabled counts as a plan-cache miss, even without a vector
-- literal, so assertion reads run under a `<case>_assert` comment and are not
-- counted by the case. `QueryCacheHits` must stay 0, so every hit belongs to
-- the plan cache and not to the result cache.

DROP TABLE IF EXISTS vector_plan_cache_run_state;
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
DROP TABLE IF EXISTS vector_plan_cache_invalidation_renamed;
DROP TABLE IF EXISTS vector_plan_cache_unrelated;
DROP TABLE IF EXISTS vector_plan_cache_unrelated_2;
DROP DATABASE IF EXISTS vector_plan_cache_other_db;

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 1;
SET enable_vector_performance_test = 1;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE vector_plan_cache_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO vector_plan_cache_run_state SELECT now64(6);

CREATE TABLE vector_plan_cache_unrelated (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_unrelated VALUES (1);

-- I01: ALTER ADD COLUMN empties every entry of the altered table, for an
-- `L2Distance` template and for a `cosineDistance` template alike. The next
-- execution of each template misses and is correct.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i01';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i01_assert';
SELECT 'I01 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i01';
SET log_comment = 'vpc_invalidation_i01';
ALTER TABLE vector_plan_cache_invalidation ADD COLUMN extra UInt8;
SET log_comment = 'vpc_invalidation_i01_assert';
SELECT 'I01 entries after ALTER', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i01';
SELECT id, round(L2Distance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SELECT id, round(cosineDistance(vector, [0., 3, 1]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i01_assert';
SYSTEM FLUSH LOGS;
SELECT 'I01 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i01'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i01_assert';
SELECT 'I01 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i01';

-- I05: DROP followed by CREATE of the same name empties the entry, and the
-- recreated table returns its new contents instead of a stale plan.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i05';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_invalidation_i05_assert';
SELECT 'I05 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i05';
SET log_comment = 'vpc_invalidation_i05';
DROP TABLE vector_plan_cache_invalidation;
SET log_comment = 'vpc_invalidation_i05_assert';
SELECT 'I05 entries after DROP', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i05';
SET log_comment = 'vpc_invalidation_i05';
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation VALUES (0, [10., 13, 11]);
SET log_comment = 'vpc_invalidation_i05_assert';
SELECT 'I05 entries after CREATE and INSERT', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i05';
SELECT id, round(L2Distance(vector, [10., 13, 11]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_invalidation_i05_assert';
SYSTEM FLUSH LOGS;
SELECT 'I05 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i05'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i05_assert';
SELECT 'I05 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i05';

-- I07: TRUNCATE empties the entry. Rows are then written by a session with
-- the cache disabled, and the next query of the same template misses and
-- sees only the new rows.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i07';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [20., 23, 21]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i07_assert';
SELECT 'I07 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i07';
SET log_comment = 'vpc_invalidation_i07';
TRUNCATE TABLE vector_plan_cache_invalidation;
SET log_comment = 'vpc_invalidation_i07_assert';
SELECT 'I07 entries after TRUNCATE', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i07';
SET log_comment = 'vpc_invalidation_i07';
SET vector_query_plan_cache = 0;
INSERT INTO vector_plan_cache_invalidation
SELECT number + 200, [toFloat32(number + 200), toFloat32(number + 203), toFloat32(number + 201)]
FROM numbers(20);
SET vector_query_plan_cache = 1;
SELECT id, round(L2Distance(vector, [20., 23, 21]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i07_assert';
SELECT 'I07 rows after TRUNCATE and INSERT', count(), min(id), max(id) FROM vector_plan_cache_invalidation;
SET log_comment = 'vpc_invalidation_i07_assert';
SYSTEM FLUSH LOGS;
SELECT 'I07 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i07'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i07_assert';
SELECT 'I07 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i07';

-- I08: RENAME in both directions empties the entry; the next query misses.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i08';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [30., 33, 31]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i08_assert';
SELECT 'I08 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i08';
SET log_comment = 'vpc_invalidation_i08';
RENAME TABLE vector_plan_cache_invalidation TO vector_plan_cache_invalidation_renamed;
SET log_comment = 'vpc_invalidation_i08_assert';
SELECT 'I08 entries after RENAME', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i08';
SET log_comment = 'vpc_invalidation_i08';
RENAME TABLE vector_plan_cache_invalidation_renamed TO vector_plan_cache_invalidation;
SET log_comment = 'vpc_invalidation_i08_assert';
SELECT 'I08 entries after RENAME back', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i08';
SELECT id, round(L2Distance(vector, [30., 33, 31]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i08_assert';
SYSTEM FLUSH LOGS;
SELECT 'I08 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i08'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i08_assert';
SELECT 'I08 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i08';

-- I09a: an INSERT in a session with the cache enabled empties the entry of
-- the target table; the new row enters the Top-K of the next execution.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i09';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [40., 43, 41]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_invalidation_i09_assert';
SELECT 'I09 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i09';
SET log_comment = 'vpc_invalidation_i09';
INSERT INTO vector_plan_cache_invalidation VALUES (1000, [40., 43, 41]);
SET log_comment = 'vpc_invalidation_i09_assert';
SELECT 'I09 entries after INSERT', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i09';
SELECT id, round(L2Distance(vector, [40., 43, 41]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5;
SET log_comment = 'vpc_invalidation_i09_assert';
SYSTEM FLUSH LOGS;
SELECT 'I09 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i09'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i09_assert';
SELECT 'I09 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i09';

-- I10: DDL on an unrelated table must not touch the entry of the queried
-- table, so the second execution of the template still hits.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i10';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i10_assert';
SELECT 'I10 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i10';
SET log_comment = 'vpc_invalidation_i10';
ALTER TABLE vector_plan_cache_unrelated ADD COLUMN v UInt8;
CREATE TABLE vector_plan_cache_unrelated_2 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_unrelated_2 VALUES (1);
DROP TABLE vector_plan_cache_unrelated_2;
SET log_comment = 'vpc_invalidation_i10_assert';
SELECT 'I10 entries after unrelated DDL', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i10';
SELECT id, round(L2Distance(vector, [50., 53, 51]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i10_assert';
SYSTEM FLUSH LOGS;
SELECT 'I10 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i10'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i10_assert';
SELECT 'I10 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i10';

-- I14: DDL in another database must not touch the entry of the queried
-- table; the second execution of the template still hits.
-- Restore the queried table to 100 rows. These statements do not emit
-- plan-cache events, because none of them is a SELECT.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
CREATE TABLE vector_plan_cache_invalidation (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_invalidation
SELECT number, [toFloat32(number), toFloat32(number + 3), toFloat32(number + 1)]
FROM numbers(100);
SET log_comment = 'vpc_invalidation_i14';
SYSTEM DROP VECTOR QUERY PLAN CACHE;
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i14_assert';
SELECT 'I14 entries after first query', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i14';
SET log_comment = 'vpc_invalidation_i14';
CREATE DATABASE vector_plan_cache_other_db;
CREATE TABLE vector_plan_cache_other_db.t (id UInt32, vector Array(Float32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO vector_plan_cache_other_db.t VALUES (1, [1., 2, 3]);
DROP DATABASE vector_plan_cache_other_db;
SET log_comment = 'vpc_invalidation_i14_assert';
SELECT 'I14 entries after DDL in another database', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i14';
SELECT id, round(L2Distance(vector, [60., 63, 61]), 4) AS d FROM vector_plan_cache_invalidation ORDER BY d, id LIMIT 5 FORMAT Null;
SET log_comment = 'vpc_invalidation_i14_assert';
SYSTEM FLUSH LOGS;
SELECT 'I14 hits, misses, result cache hits',
    sum(ProfileEvents['VectorQueryPlanCacheHits']),
    sum(ProfileEvents['VectorQueryPlanCacheMisses']),
    sum(ProfileEvents['QueryCacheHits'])
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = 'vpc_invalidation_i14'
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM vector_plan_cache_run_state);
SET log_comment = 'vpc_invalidation_i14_assert';
SELECT 'I14 entries at end', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';
SET log_comment = 'vpc_invalidation_i14';

-- Cleanup.
SET log_comment = 'vpc_invalidation_setup';
DROP TABLE IF EXISTS vector_plan_cache_invalidation;
DROP TABLE IF EXISTS vector_plan_cache_invalidation_renamed;
DROP TABLE IF EXISTS vector_plan_cache_unrelated;
DROP TABLE IF EXISTS vector_plan_cache_unrelated_2;
DROP DATABASE IF EXISTS vector_plan_cache_other_db;
DROP TABLE vector_plan_cache_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
