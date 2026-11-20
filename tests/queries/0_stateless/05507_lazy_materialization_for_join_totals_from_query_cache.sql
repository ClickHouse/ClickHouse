-- Lazy materialization over joins is not used where a subquery result read from the query result cache has totals:
-- the cached block of totals goes through the joins up to the LIMIT with no rows to read lazily.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
CREATE TABLE l (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number, concat('p', toString(number)) FROM numbers(10);
INSERT INTO r SELECT number % 5, number FROM numbers(20);

SELECT '-- WITH TOTALS on the right side, the second run reads the subquery result from the cache';
SELECT /* 05507 right */ l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS x
ON l.k = x.k ORDER BY l.k LIMIT 2;
SELECT /* 05507 right */ l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS x
ON l.k = x.k ORDER BY l.k LIMIT 2;

SELECT '-- WITH TOTALS on the left side, the second run reads the subquery result from the cache';
SELECT /* 05507 left */ x.k, x.s, l.payload FROM (SELECT k, sum(v) AS s FROM r GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS x
JOIN l ON x.k = l.k ORDER BY x.k LIMIT 2;
SELECT /* 05507 left */ x.k, x.s, l.payload FROM (SELECT k, sum(v) AS s FROM r GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS x
JOIN l ON x.k = l.k ORDER BY x.k LIMIT 2;

SELECT '-- a run read from the cache without totals still reads lazily';
SELECT /* 05507 nototals */ l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k SETTINGS use_query_cache = 1) AS x
ON l.k = x.k ORDER BY x.c, l.k LIMIT 2 SETTINGS log_processors_profiles = 1 FORMAT Null;
SELECT /* 05507 nototals */ l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k SETTINGS use_query_cache = 1) AS x
ON l.k = x.k ORDER BY x.c, l.k LIMIT 2 SETTINGS log_processors_profiles = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT '-- a run of each was served from the cache';
SELECT
    countIf(query LIKE '%/* 05507 right */%' AND ProfileEvents['QueryCacheHits'] > 0) > 0,
    countIf(query LIKE '%/* 05507 left */%' AND ProfileEvents['QueryCacheHits'] > 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
SELECT countIf(q.query LIKE '%/* 05507 nototals */%' AND q.ProfileEvents['QueryCacheHits'] > 0 AND p.lazy > 0) > 0
FROM system.query_log q
LEFT JOIN (SELECT query_id, countIf(name = 'LazyMaterializingTransform') AS lazy FROM system.processors_profile_log GROUP BY query_id) AS p
ON q.query_id = p.query_id
WHERE q.current_database = currentDatabase() AND q.type = 'QueryFinish' AND q.query_kind = 'Select';

DROP TABLE l;
DROP TABLE r;
