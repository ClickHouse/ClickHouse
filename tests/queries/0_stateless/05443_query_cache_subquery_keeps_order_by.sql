-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- A cached subquery result must keep the order of the ORDER BY of the subquery, also when the query that stored it did not need that order.
-- https://github.com/ClickHouse/ClickHouse/issues/126872

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_query_cache_order;
CREATE TABLE t_query_cache_order (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_query_cache_order SELECT number FROM numbers(10);

-- Neither the outer aggregation nor the outer ORDER BY depends on the order of the subquery.
SELECT sum(x) FROM (SELECT x FROM t_query_cache_order ORDER BY x DESC SETTINGS use_query_cache = 1);
SELECT y FROM (SELECT x + 100 AS y FROM t_query_cache_order ORDER BY y DESC SETTINGS use_query_cache = 1) ORDER BY y FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery = 1;

-- The table is empty now, so the subqueries below are answered from the cache.
TRUNCATE TABLE t_query_cache_order;
SELECT groupArray(x) FROM (SELECT x FROM t_query_cache_order ORDER BY x DESC SETTINGS use_query_cache = 1);
SELECT y FROM (SELECT x + 100 AS y FROM t_query_cache_order ORDER BY y DESC SETTINGS use_query_cache = 1) LIMIT 3;

DROP TABLE t_query_cache_order;

SYSTEM DROP QUERY CACHE;
