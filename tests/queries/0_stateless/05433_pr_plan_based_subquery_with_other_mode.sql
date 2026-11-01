-- Query-based and plan-based parallel replicas cannot be mixed in one query: a subquery setting a different
-- `parallel_replicas_plan_based` is rejected, unless parallel replicas are disabled for it.

DROP TABLE IF EXISTS t_05433;
CREATE TABLE t_05433 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05433 SELECT number, number % 1000 FROM numbers(200000);

SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 0;

-- The same setting in the subquery: the read step matches it and aggregation stays on the replicas.
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

-- `IN` subquery.
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0; -- { serverError INCORRECT_QUERY }
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 0)
SETTINGS parallel_replicas_plan_based = 1; -- { serverError INCORRECT_QUERY }

-- Root `UNION` arm.
SET parallel_replicas_plan_based = 0;
(SELECT sum(x) FROM t_05433 SETTINGS parallel_replicas_plan_based = 1) UNION ALL SELECT sum(x) FROM t_05433; -- { serverError INCORRECT_QUERY }
SET parallel_replicas_plan_based = 1;
(SELECT sum(x) FROM t_05433 SETTINGS parallel_replicas_plan_based = 0) UNION ALL SELECT sum(x) FROM t_05433; -- { serverError INCORRECT_QUERY }
SET parallel_replicas_plan_based = DEFAULT;

-- Automatic mode.
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0)
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 1, parallel_replicas_plan_based = 1; -- { serverError INCORRECT_QUERY }

-- Parallel replicas disabled for the query.
SELECT '-- without parallel replicas';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1)
SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 0;
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 0)
SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 1;

-- Parallel replicas disabled for the subquery.
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0;
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 0)
SETTINGS parallel_replicas_plan_based = 1;
SELECT count() FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0;
SET parallel_replicas_plan_based = 0;
(SELECT sum(x) FROM t_05433 SETTINGS enable_parallel_replicas = 0, parallel_replicas_plan_based = 1) UNION ALL SELECT sum(x) FROM t_05433;
SET parallel_replicas_plan_based = DEFAULT;

-- Distributed `INSERT SELECT` is query-based, so the setting has no effect there.
SELECT '-- distributed insert';
DROP TABLE IF EXISTS t_05433_dst;
CREATE TABLE t_05433_dst (x UInt64) ENGINE = Memory;
INSERT INTO t_05433_dst SELECT x FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 1, parallel_distributed_insert_select = 2;
SELECT count() FROM t_05433_dst;
INSERT INTO t_05433_dst SELECT x FROM t_05433 WHERE x IN (SELECT x FROM t_05433 WHERE y < 5 SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0, parallel_distributed_insert_select = 2;
SELECT count() FROM t_05433_dst;
DROP TABLE t_05433_dst;

-- The query's setting applies to its subqueries.
SELECT '-- whole query';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y) SETTINGS parallel_replicas_plan_based = 1;
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y) SETTINGS parallel_replicas_plan_based = 0;

DROP TABLE t_05433;
