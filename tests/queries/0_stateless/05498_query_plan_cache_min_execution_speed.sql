-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
-- The reads of a plan executed through the query plan cache are resolved at the `FetchColumns`
-- stage, which does not carry the minimum speed limits checked only by the initiator of a query.
-- They must still be enforced: a query that is too slow without the cache must be too slow on a
-- miss and on a hit, too.
-- The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489).

SYSTEM DROP QUERY PLAN CACHE;

DROP TABLE IF EXISTS t_min_speed;
CREATE TABLE t_min_speed (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_min_speed SELECT number FROM numbers(100000);

-- `use_statistics_for_sum_avg_aggregation` would answer `sum` from part statistics without reading.
SET timeout_before_checking_execution_speed = 0, max_block_size = 1000, use_statistics_for_sum_avg_aggregation = 0;

SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed = 1000000000000000, log_comment = '05498_rows_off'; -- { serverError TOO_SLOW }
SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed_bytes = 1000000000000000, log_comment = '05498_bytes_off'; -- { serverError TOO_SLOW }

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed = 1000000000000000, log_comment = '05498_rows_miss'; -- { serverError TOO_SLOW }
SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed = 1000000000000000, log_comment = '05498_rows_hit'; -- { serverError TOO_SLOW }
SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed_bytes = 1000000000000000, log_comment = '05498_bytes_miss'; -- { serverError TOO_SLOW }
SELECT sum(id) FROM t_min_speed SETTINGS min_execution_speed_bytes = 1000000000000000, log_comment = '05498_bytes_hit'; -- { serverError TOO_SLOW }

-- The limits apply to the reads of `IN` subqueries, too. The set is built before the query starts.
SELECT count() FROM t_min_speed WHERE id IN (SELECT id FROM t_min_speed) SETTINGS min_execution_speed = 1000000000000000, log_comment = '05498_set_miss'; -- { serverError TOO_SLOW }
SELECT count() FROM t_min_speed WHERE id IN (SELECT id FROM t_min_speed) SETTINGS min_execution_speed = 1000000000000000, log_comment = '05498_set_hit'; -- { serverError TOO_SLOW }

-- Without the limits the cached plan still works.
SELECT sum(id) FROM t_min_speed SETTINGS log_comment = '05498_no_limit_miss';
SELECT sum(id) FROM t_min_speed SETTINGS log_comment = '05498_no_limit_hit';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, type, ProfileEvents['QueryPlanCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') AND log_comment LIKE '05498\\_%' AND log_comment NOT LIKE '%.sql%'
ORDER BY event_time_microseconds;

DROP TABLE t_min_speed;
