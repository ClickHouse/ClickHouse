-- Tags: no-parallel, no-random-settings, no-old-analyzer, no-parallel-replicas
-- no-parallel: the query plan cache is server-wide and is cleared here.
-- no-random-settings: whether the second run hits depends on the exact logical plan.

-- With one thread the planner drops the reshuffle between windows of different partitioning. A plan cached
-- by a single-threaded run must still keep it, otherwise a hit with more threads splits a partition of the
-- second window across streams and computes it partially.

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

DROP TABLE IF EXISTS t_window_threads;
CREATE TABLE t_window_threads (a UInt64, b UInt64, c UInt64, x UInt64) ENGINE = MergeTree ORDER BY c;
SYSTEM STOP MERGES t_window_threads;
INSERT INTO t_window_threads SELECT 1, number, number, 1 FROM numbers(0, 2500);
INSERT INTO t_window_threads SELECT 1, number, number, 1 FROM numbers(2500, 2500);
INSERT INTO t_window_threads SELECT 1, number, number, 1 FROM numbers(5000, 2500);
INSERT INTO t_window_threads SELECT 1, number, number, 1 FROM numbers(7500, 2500);

SYSTEM DROP QUERY PLAN CACHE;

SELECT min(s1), max(s1), min(s2), max(s2) FROM
(
    SELECT
        sum(x) OVER (PARTITION BY a, b ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s1,
        sum(x) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s2
    FROM t_window_threads
)
SETTINGS max_threads = 1, log_comment = '05409_warm';

SELECT min(s1), max(s1), min(s2), max(s2) FROM
(
    SELECT
        sum(x) OVER (PARTITION BY a, b ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s1,
        sum(x) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s2
    FROM t_window_threads
)
SETTINGS max_threads = 4, log_comment = '05409_hit';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryPlanCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05409_warm', '05409_hit')
ORDER BY event_time_microseconds;

DROP TABLE t_window_threads;
