-- When the planner decides not to use plan-based parallel replicas, the plan has no split aggregation.
-- https://github.com/ClickHouse/ClickHouse/issues/128539

DROP TABLE IF EXISTS t_pr_no_split;
DROP TABLE IF EXISTS t_pr_no_split_dim;
DROP TABLE IF EXISTS t_pr_no_split_merge;

CREATE TABLE t_pr_no_split (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_no_split SELECT number, number FROM numbers(100000);
CREATE TABLE t_pr_no_split_dim (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_no_split_dim SELECT number * 2 FROM numbers(1000);
CREATE TABLE t_pr_no_split_merge AS t_pr_no_split ENGINE = Merge(currentDatabase(), '^t_pr_no_split(_dim)?$');

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Not enough rows for parallel replicas.
SELECT countIf(explain LIKE '%Aggregating%'), countIf(explain LIKE '%MergingAggregated%'), countIf(explain LIKE '%ParallelReplicas%')
FROM (EXPLAIN SELECT x % 10 AS k, sum(y) FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000000000);

SELECT sum(s) FROM (SELECT x % 10 AS k, sum(y) AS s FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000000000);

-- Enough rows: the aggregation is split and distributed.
SELECT countIf(explain LIKE '%MergingAggregated%') > 0, countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN SELECT x % 10 AS k, sum(y) FROM t_pr_no_split GROUP BY k SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0);

-- The local shard of a `remote` query does not use parallel replicas either.
SELECT countIf(explain LIKE '%MergingAggregated%'), countIf(explain LIKE '%ParallelReplicasSplit%')
FROM (EXPLAIN SELECT * FROM (SELECT x, sum(y) FROM remote('127.0.0.{1,2}', currentDatabase(), t_pr_no_split) GROUP BY x) WHERE x = 42
    SETTINGS prefer_localhost_replica = 1);

-- A read which is only shipped does not need parallel replicas in its own context: the broadcast side of a join
-- and the read of an `IN` set are executed whole by every replica.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%'), countIf(explain LIKE '%JoinLogical%')
FROM (EXPLAIN SELECT sum(t.y) FROM t_pr_no_split AS t INNER JOIN (SELECT x FROM t_pr_no_split_dim SETTINGS enable_parallel_replicas = 0) AS d ON t.x = d.x
    SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0, query_plan_join_swap_table = 'false');

SELECT countIf(explain LIKE '%ReadFromParallelReplicas%')
FROM (EXPLAIN SELECT sum(y) FROM t_pr_no_split WHERE x IN (SELECT x FROM t_pr_no_split_dim SETTINGS enable_parallel_replicas = 0)
    SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0);

-- A `Merge` read whose context disables parallel replicas is not expanded into the reads of its tables.
SELECT countIf(explain LIKE '%Union%'), countIf(explain LIKE '%ReadFromParallelReplicas%')
FROM (EXPLAIN SELECT g, count() FROM (SELECT x % 10 AS g FROM t_pr_no_split_merge SETTINGS enable_parallel_replicas = 0) GROUP BY g
    SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0, parallel_replicas_allow_merge_tables = 1);

SELECT countIf(explain LIKE '%Union%') > 0, countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN SELECT g, count() FROM (SELECT x % 10 AS g FROM t_pr_no_split_merge) GROUP BY g
    SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0, parallel_replicas_allow_merge_tables = 1);

DROP TABLE t_pr_no_split_merge;
DROP TABLE t_pr_no_split;
DROP TABLE t_pr_no_split_dim;
