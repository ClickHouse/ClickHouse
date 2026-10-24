-- One query reads with replicas using one implementation. A subquery carries its own context, so its
-- `SETTINGS` clause can name the other one, but the two split the work at different points and a plan
-- mixing them loses parallel replicas for the subtree instead of combining them. So such a query is
-- rejected while parallel replicas can be used, and runs as usual when they cannot.

DROP TABLE IF EXISTS t_05433;
CREATE TABLE t_05433 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05433 SELECT number, number % 1000 FROM numbers(200000);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 0;

-- The subquery repeating the query's implementation is not a mix: the read step is the one of that
-- implementation, and the aggregation stays where the data is read.
SELECT '-- query-based';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0))
SETTINGS parallel_replicas_plan_based = 0;

SELECT '-- plan-based';
SELECT countIf(explain ILIKE '%ReadFromParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1))
SETTINGS parallel_replicas_plan_based = 1;

SELECT '-- mixed';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0; -- { serverError INCORRECT_QUERY }
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0)
SETTINGS parallel_replicas_plan_based = 1; -- { serverError INCORRECT_QUERY }

-- A subquery that builds an `IN` set has its own context and its own planner too.
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0; -- { serverError INCORRECT_QUERY }
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 0)
SETTINGS parallel_replicas_plan_based = 1; -- { serverError INCORRECT_QUERY }

-- So does the automatic mode, which decides on parallel replicas only while optimizing the plan.
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0)
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 1, parallel_replicas_plan_based = 1; -- { serverError INCORRECT_QUERY }

-- Without parallel replicas the setting has no effect, so the same queries run.
SELECT '-- without parallel replicas';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1)
SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 0;
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 0)
SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 1;

-- Setting the implementation for the whole query reaches its subqueries.
SELECT '-- whole query';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y) SETTINGS parallel_replicas_plan_based = 1;
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y) SETTINGS parallel_replicas_plan_based = 0;

DROP TABLE t_05433;
