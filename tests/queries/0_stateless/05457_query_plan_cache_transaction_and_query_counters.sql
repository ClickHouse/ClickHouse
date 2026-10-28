-- Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database
-- A query plan cache hit skips the planner, which checks that every storage read inside a transaction
-- supports transactions. A plan warmed outside of a transaction must not let a `Memory` table be read
-- inside one. The plan cache path also constructs its interpreters directly, so it must keep the query
-- counters (`Query`, `SelectQuery`, ...) of `InterpreterFactory`. The plan cache is a single,
-- server-wide cache, so the test runs in isolation (see 04489 for the full rationale of the tags).

SYSTEM DROP QUERY PLAN CACHE;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64) ENGINE = Memory;
INSERT INTO t VALUES (1), (2), (3);

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

SELECT sum(id) FROM t SETTINGS log_comment = '05457_warm';
SELECT sum(id) FROM t SETTINGS log_comment = '05457_hit';

BEGIN TRANSACTION;
SELECT sum(id) FROM t SETTINGS log_comment = '05457_in_transaction'; -- { serverError NOT_IMPLEMENTED }
ROLLBACK;

SELECT sum(id) FROM t SETTINGS log_comment = '05457_off', enable_query_plan_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryPlanCacheHits'], ProfileEvents['Query'], ProfileEvents['SelectQuery'],
    ProfileEvents['InitialQuery'], ProfileEvents['InitialSelectQuery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05457_warm', '05457_hit', '05457_off')
ORDER BY event_time_microseconds;

DROP TABLE t;
