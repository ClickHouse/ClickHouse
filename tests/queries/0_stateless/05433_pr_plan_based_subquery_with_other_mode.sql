-- One query reads with replicas using one implementation. A subquery carries its own context, so its
-- `SETTINGS` clause can name the other one - and the query's choice has to win anyway, because the two
-- split the work at different points and a plan mixing them loses parallel replicas for the subtree
-- instead of combining them.
--
-- Each pair of rows below is the same query with the subquery asking for either implementation: the
-- read step is the one the query itself asks for, and the aggregation stays where the data is read,
-- which the merging step above the read reports.

DROP TABLE IF EXISTS t_05433;
CREATE TABLE t_05433 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05433 SELECT number, number % 1000 FROM numbers(200000);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 0;

SELECT '-- query-based';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0))
SETTINGS parallel_replicas_plan_based = 0;

SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1))
SETTINGS parallel_replicas_plan_based = 0;

SELECT '-- plan-based';
SELECT countIf(explain ILIKE '%ReadFromParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0))
SETTINGS parallel_replicas_plan_based = 1;

SELECT countIf(explain ILIKE '%ReadFromParallelReplicas%') > 0, countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') = 0, countIf(explain ILIKE '%MergingAggregated%') > 0
FROM (EXPLAIN description = 0 SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1))
SETTINGS parallel_replicas_plan_based = 1;

SELECT '-- results';
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 1)
SETTINGS parallel_replicas_plan_based = 0;
SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0)
SETTINGS parallel_replicas_plan_based = 1;

DROP TABLE t_05433;
