-- An `IN` set built from a temporary table under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- Every replica builds a subquery set by running its plan, and a temporary table exists only in the session of the
-- initiator, so a shipped set plan fails on the replica (`DATABASE_ACCESS_DENIED`). The plan fragment stays local.
-- Without the local plan every replica is a remote one, so a shipped set plan would always reach one.
-- https://github.com/ClickHouse/ClickHouse/issues/129273
-- A temporary `Set` table is shipped by name instead, and the replica refuses it the same way.
-- https://github.com/ClickHouse/ClickHouse/issues/132348

DROP TABLE IF EXISTS t_pr_in_tmp;

CREATE TABLE t_pr_in_tmp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_pr_in_tmp SELECT number, number % 100 FROM numbers(100000);

CREATE TEMPORARY TABLE tmp_pr_in (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tmp_pr_in SELECT number * 7 FROM numbers(1000);

CREATE TEMPORARY TABLE tmp_pr_in_set (k UInt64) ENGINE = Set;
INSERT INTO tmp_pr_in_set SELECT number * 7 FROM numbers(1000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 0;

SELECT count(), sum(v) FROM t_pr_in_tmp WHERE k IN (SELECT k FROM tmp_pr_in);
SELECT count() FROM t_pr_in_tmp WHERE k NOT IN (SELECT k FROM tmp_pr_in);
-- The sets nested in the plan of a set are shipped with it.
SELECT count() FROM t_pr_in_tmp WHERE k IN (SELECT k FROM t_pr_in_tmp WHERE k IN (SELECT k FROM tmp_pr_in));

SELECT count(), sum(v) FROM t_pr_in_tmp WHERE k IN tmp_pr_in_set;
SELECT count() FROM t_pr_in_tmp WHERE k NOT IN tmp_pr_in_set;
SELECT count() FROM t_pr_in_tmp WHERE k IN (SELECT k FROM t_pr_in_tmp WHERE k IN tmp_pr_in_set);

DROP TABLE t_pr_in_tmp;
