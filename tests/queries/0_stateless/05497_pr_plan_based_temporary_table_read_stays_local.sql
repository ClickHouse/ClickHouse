-- A temporary table exists only in the session of the initiator, so plan-based parallel replicas must not put its
-- read into a shipped fragment: a replica refuses the temporary database with `DATABASE_ACCESS_DENIED`. The other
-- subtrees of the query can still be distributed.

DROP TABLE IF EXISTS t_pr_regular;

CREATE TABLE t_pr_regular (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_regular SELECT number, number * 10 FROM numbers(1000);

CREATE TEMPORARY TABLE t_pr_tmp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_tmp SELECT number, number * 2 FROM numbers(1000);

CREATE TEMPORARY TABLE t_pr_tmp_memory (k UInt64, v UInt64) ENGINE = Memory;
INSERT INTO t_pr_tmp_memory SELECT number, number * 2 FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
-- Without the local plan every replica, the local one included, runs a shipped fragment, so a shipped read of
-- the temporary table always fails.
SET parallel_replicas_local_plan = 0;
-- Only the left side of a join is distributed; keep the sides as written.
SET query_plan_join_swap_table = 'false';

SELECT 'temporary table is read locally';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%')
FROM (EXPLAIN description = 0 SELECT sum(v) FROM t_pr_tmp);
SELECT sum(v) FROM t_pr_tmp;
SELECT k, v FROM t_pr_tmp WHERE k IN (1, 500, 999) ORDER BY k;

SELECT 'aggregation joined with a temporary table: the aggregation is distributed, the join is local';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN description = 0
    SELECT count(), sum(a.s + t.v) FROM (SELECT k, sum(v) AS s FROM t_pr_regular GROUP BY k) AS a INNER JOIN t_pr_tmp AS t ON a.k = t.k);
SELECT count(), sum(a.s + t.v) FROM (SELECT k, sum(v) AS s FROM t_pr_regular GROUP BY k) AS a INNER JOIN t_pr_tmp AS t ON a.k = t.k;

SELECT 'joins with a temporary table';
SELECT count(), sum(r.v + t.v) FROM t_pr_regular AS r INNER JOIN t_pr_tmp AS t ON r.k = t.k;
SELECT count(), sum(t.v) FROM t_pr_tmp AS t INNER JOIN t_pr_regular AS r ON r.k = t.k WHERE r.v < 100;

SELECT 'joins with a Memory temporary table: its read is not serializable, so it stays local';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN description = 0 SELECT count(), sum(r.v + m.v) FROM t_pr_regular AS r INNER JOIN t_pr_tmp_memory AS m ON r.k = m.k);
SELECT count(), sum(r.v + m.v) FROM t_pr_regular AS r INNER JOIN t_pr_tmp_memory AS m ON r.k = m.k;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%')
FROM (EXPLAIN description = 0 SELECT count(), sum(r.v + m.v) FROM t_pr_tmp_memory AS m INNER JOIN t_pr_regular AS r ON r.k = m.k);
SELECT count(), sum(r.v + m.v) FROM t_pr_tmp_memory AS m INNER JOIN t_pr_regular AS r ON r.k = m.k;

DROP TABLE t_pr_regular;
