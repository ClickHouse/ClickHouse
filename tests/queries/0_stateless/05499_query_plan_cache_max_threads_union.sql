-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
-- A query plan cache hit must be limited by the `max_threads` of the current query. The cap stored
-- with the plan is replaced, not cleared: a plan without a cap is not limited at all, and a
-- `UNION ALL` of two one-stream reads would then run on two threads with `max_threads = 1`.
-- The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489).

SYSTEM DROP QUERY PLAN CACHE;

DROP TABLE IF EXISTS t_max_threads_union;
CREATE TABLE t_max_threads_union (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_max_threads_union SELECT number FROM numbers(100000);

SELECT sum(id) FROM (SELECT id FROM t_max_threads_union UNION ALL SELECT id FROM t_max_threads_union)
SETTINGS max_threads = 1, log_comment = '05499_off';

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

-- `max_threads` is not part of the cache key: the entry stored with one value is hit with another.
SELECT sum(id) FROM (SELECT id FROM t_max_threads_union UNION ALL SELECT id FROM t_max_threads_union)
SETTINGS max_threads = 4, log_comment = '05499_miss';
SELECT sum(id) FROM (SELECT id FROM t_max_threads_union UNION ALL SELECT id FROM t_max_threads_union)
SETTINGS max_threads = 1, log_comment = '05499_hit';

SYSTEM FLUSH LOGS query_log;

SELECT
    sumIf(ProfileEvents['QueryPlanCacheHits'], log_comment = '05499_hit') AS hits,
    maxIf(peak_threads_usage, log_comment = '05499_hit') <= maxIf(peak_threads_usage, log_comment = '05499_off') AS limited
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05499_off', '05499_miss', '05499_hit');

DROP TABLE t_max_threads_union;
