-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
-- A non-deterministic built-in function passed by name to a higher-order function, e.g.
-- `arrayMap(randConstant, [1])`, is turned into a lambda by the analyzer and folded into a constant:
-- a cached plan would keep serving that value. Such a query, also inside an expanded view, must not
-- be served from the query plan cache.
-- The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489).

SYSTEM DROP QUERY PLAN CACHE;

DROP VIEW IF EXISTS v_rand_constant;
DROP TABLE IF EXISTS t_rand_constant;

CREATE TABLE t_rand_constant (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_rand_constant VALUES (1);

CREATE VIEW v_rand_constant AS SELECT arrayMap(randConstant, [1]) AS x FROM t_rand_constant;

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

SELECT length(x) FROM v_rand_constant SETTINGS log_comment = '05497_view_1';
SELECT length(x) FROM v_rand_constant SETTINGS log_comment = '05497_view_2';
SELECT length(arrayMap(randConstant, [id])) FROM t_rand_constant SETTINGS log_comment = '05497_query_1';
SELECT length(arrayMap(randConstant, [id])) FROM t_rand_constant SETTINGS log_comment = '05497_query_2';
-- A deterministic function passed by name stays cacheable.
SELECT arrayMap(toString, [id]) FROM t_rand_constant SETTINGS log_comment = '05497_deterministic_1';
SELECT arrayMap(toString, [id]) FROM t_rand_constant SETTINGS log_comment = '05497_deterministic_2';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryPlanCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05497\\_%' AND log_comment NOT LIKE '%.sql%'
ORDER BY event_time_microseconds;

DROP VIEW v_rand_constant;
DROP TABLE t_rand_constant;
