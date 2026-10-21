-- A subquery can set `parallel_replicas_plan_based` to the opposite of the outer query's value. The
-- planner's query-tree analysis, which the query-based implementation uses to pick the query to read
-- with replicas, is skipped when the outer query is plan-based - and must not leave such a subquery
-- reading rows and aggregating them on the initiator. Both halves of the check below say the
-- aggregation still happens where the data is read: the plan merges partial aggregates on top of a
-- parallel replicas read, whichever implementation serves it.

DROP TABLE IF EXISTS t_05433;
CREATE TABLE t_05433 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05433 SELECT number, number % 1000 FROM numbers(200000);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0;

SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0);

SELECT countIf(explain ILIKE '%MergingAggregated%') > 0 AND countIf(explain ILIKE '%ParallelReplicas%') > 0
FROM (
  EXPLAIN description = 0
  SELECT sum(c) FROM (SELECT y, count() AS c FROM t_05433 GROUP BY y SETTINGS parallel_replicas_plan_based = 0)
);

DROP TABLE t_05433;
