-- A `LIMIT` without `ORDER BY` under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- The `LIMIT` stays on the initiator, and a copy of it, `limit + offset`, ships inside the fragment as
-- `Limit (local LIMIT)`. Without that copy every replica reads its whole share of the table and the initiator
-- cancels it once it has enough rows, so a cancelled replica never reports its progress (see
-- `02841_parallel_replicas_summary`). Whether a replica is cancelled is a matter of timing, so the plan is
-- what is checked; the results are compared with classic parallel replicas.

DROP TABLE IF EXISTS t_pr_ship_limit;
DROP TABLE IF EXISTS t_pr_ship_limit_2;

CREATE TABLE t_pr_ship_limit (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 128;
CREATE TABLE t_pr_ship_limit_2 (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 128;
INSERT INTO t_pr_ship_limit SELECT number FROM numbers(100000);
INSERT INTO t_pr_ship_limit_2 SELECT number FROM numbers(100000);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 0;
SET exact_rows_before_limit = 0;

SELECT '--- the fragment carries the LIMIT';
SELECT 'plain', countIf(explain LIKE '%local LIMIT%') > 0 FROM (EXPLAIN PLAN SELECT * FROM t_pr_ship_limit LIMIT 100);
SELECT 'offset', countIf(explain LIKE '%local LIMIT%') > 0 FROM (EXPLAIN PLAN SELECT * FROM t_pr_ship_limit LIMIT 100 OFFSET 50);
SELECT 'filter', countIf(explain LIKE '%local LIMIT%') > 0 FROM (EXPLAIN PLAN SELECT n + 1 FROM t_pr_ship_limit WHERE n % 3 = 0 LIMIT 100);
SELECT 'union', countIf(explain LIKE '%local LIMIT%') FROM (EXPLAIN PLAN SELECT * FROM (SELECT n FROM t_pr_ship_limit UNION ALL SELECT n FROM t_pr_ship_limit_2) LIMIT 100);

SELECT '--- when every row must be read, the LIMIT is shipped and reads till the end';
SELECT 'exact_rows_before_limit', countIf(explain LIKE '%local LIMIT%') FROM (EXPLAIN PLAN SELECT * FROM t_pr_ship_limit LIMIT 100 SETTINGS exact_rows_before_limit = 1);

SELECT '--- the same results as classic parallel replicas';
SELECT 'plain', count(), uniqExact(n) FROM (SELECT * FROM t_pr_ship_limit LIMIT 100);
SELECT 'offset', count(), uniqExact(n) FROM (SELECT * FROM t_pr_ship_limit LIMIT 100 OFFSET 50);
SELECT 'deep offset', count(), uniqExact(n) FROM (SELECT * FROM t_pr_ship_limit LIMIT 1000 OFFSET 99500);
SELECT 'filter', count(), countIf(m % 3 = 1) FROM (SELECT n + 1 AS m FROM t_pr_ship_limit WHERE n % 3 = 0 LIMIT 100);
SELECT 'deep offset, classic', count(), uniqExact(n) FROM (SELECT * FROM t_pr_ship_limit LIMIT 1000 OFFSET 99500) SETTINGS parallel_replicas_plan_based = 0;

SELECT '--- rows_before_limit_at_least';
SELECT 1 AS x FROM t_pr_ship_limit LIMIT 5 FORMAT JSON SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0;

SELECT '--- each replica returns at most limit + offset rows';
SELECT * FROM t_pr_ship_limit LIMIT 100 OFFSET 50 FORMAT Null SETTINGS log_comment = '05546_replica_rows';
SYSTEM FLUSH LOGS query_log;
SELECT 'replicas used', countIf(is_initial_query = 0) > 0, 'at most 150 rows each', maxIf(result_rows, is_initial_query = 0) <= 150
FROM system.query_log
WHERE initial_query_id = (
    SELECT query_id FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query = 1 AND current_database = currentDatabase()
      AND log_comment = '05546_replica_rows'
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
    ORDER BY event_time DESC LIMIT 1)
  AND type != 'QueryStart' AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR;

DROP TABLE t_pr_ship_limit;
DROP TABLE t_pr_ship_limit_2;
