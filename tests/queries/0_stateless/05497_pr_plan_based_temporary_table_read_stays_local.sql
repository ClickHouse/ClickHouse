-- A temporary table exists only in the session of the initiator, so plan-based parallel replicas must not ship
-- a read of it: a replica refuses the temporary database with `DATABASE_ACCESS_DENIED`.

DROP TABLE IF EXISTS t_pr_regular;

CREATE TABLE t_pr_regular (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_regular SELECT number, number * 10 FROM numbers(1000);

CREATE TEMPORARY TABLE t_pr_tmp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_tmp SELECT number, number * 2 FROM numbers(1000);

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

SELECT 'regular table is read by the replicas';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN description = 0 SELECT sum(v) FROM t_pr_regular);

SELECT 'temporary table is read locally';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%')
FROM (EXPLAIN description = 0 SELECT sum(v) FROM t_pr_tmp);
SELECT sum(v) FROM t_pr_tmp;
SELECT k, v FROM t_pr_tmp WHERE k IN (1, 500, 999) ORDER BY k;

-- `description = 0` does not print a shipped fragment, so a `Join` or `ReadFromMergeTree` step in the output runs
-- on the initiator. The legacy EXPLAIN format prints each step name alone on an indented line.
SET explain_query_plan_default = 'legacy';
SELECT 'join of regular tables is shipped: local Join steps';
SELECT countIf(trimBoth(explain) = 'Join')
FROM (EXPLAIN description = 0 SELECT count() FROM t_pr_regular AS r INNER JOIN t_pr_regular AS t ON r.k = t.k);

SELECT 'join with a temporary table runs locally: local Join steps, local reads';
SELECT countIf(trimBoth(explain) = 'Join'), countIf(trimBoth(explain) = 'ReadFromMergeTree') > 0
FROM (EXPLAIN description = 0 SELECT count(), sum(r.v + t.v) FROM t_pr_regular AS r INNER JOIN t_pr_tmp AS t ON r.k = t.k);
SELECT count(), sum(r.v + t.v) FROM t_pr_regular AS r INNER JOIN t_pr_tmp AS t ON r.k = t.k;
SELECT countIf(trimBoth(explain) = 'Join'), countIf(trimBoth(explain) = 'ReadFromMergeTree') > 0
FROM (EXPLAIN description = 0 SELECT count(), sum(t.v) FROM t_pr_tmp AS t INNER JOIN t_pr_regular AS r ON r.k = t.k WHERE r.v < 100);
SELECT count(), sum(t.v) FROM t_pr_tmp AS t INNER JOIN t_pr_regular AS r ON r.k = t.k WHERE r.v < 100;

DROP TABLE t_pr_regular;
