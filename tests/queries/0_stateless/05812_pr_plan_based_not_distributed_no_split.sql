-- When the planner decides not to use plan-based parallel replicas, the plan has no split aggregation.
-- https://github.com/ClickHouse/ClickHouse/issues/128539

DROP TABLE IF EXISTS t_pr_no_split;
DROP TABLE IF EXISTS t_pr_no_split_dim;

CREATE TABLE t_pr_no_split (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_no_split SELECT number, number FROM numbers(100000);
CREATE TABLE t_pr_no_split_dim (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_no_split_dim SELECT number * 2 FROM numbers(1000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Not enough rows for parallel replicas.
SELECT countIf(explain LIKE '%Aggregating%'), countIf(explain LIKE '%MergingAggregated%'), countIf(explain LIKE '%ParallelReplicas%')
FROM (EXPLAIN distributed = 1 SELECT x % 10 AS k, sum(y) FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000000000);

SELECT sum(s) FROM (SELECT x % 10 AS k, sum(y) AS s FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000000000);

-- Enough rows: the aggregation is split and distributed.
SELECT countIf(explain LIKE '%MergingAggregated%') > 0, countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN distributed = 1 SELECT x % 10 AS k, sum(y) FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0);

-- The local shard of a `remote` query does not use parallel replicas either.
SELECT countIf(explain LIKE '%MergingAggregated%'), countIf(explain LIKE '%ParallelReplicasSplit%')
FROM (EXPLAIN distributed = 1 SELECT * FROM (SELECT x, sum(y) FROM remote('127.0.0.{1,2}', currentDatabase(), t_pr_no_split) GROUP BY x) WHERE x = 42
    SETTINGS prefer_localhost_replica = 1);

-- A subquery is planned in its own context, so the planner disables parallel replicas for the small one alone. A join
-- with it on the broadcast side is still shipped: `JoinLogical` is only in the shipped fragment.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%'), countIf(explain LIKE '%JoinLogical%')
FROM (EXPLAIN distributed = 1 SELECT sum(t.y) FROM t_pr_no_split AS t INNER JOIN (SELECT x FROM t_pr_no_split_dim) AS d ON t.x = d.x
    SETTINGS parallel_replicas_min_number_of_rows_per_replica = 10000, query_plan_join_swap_table = 'false');

DROP TABLE t_pr_no_split;
DROP TABLE t_pr_no_split_dim;
