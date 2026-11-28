-- A scalar subquery in `PREWHERE` must reach a plan-based parallel replicas fragment as a value: the `PREWHERE` is a DAG
-- of the read step of its own, and the fragment carries only the shard scalars, not the scalars of the query.
DROP TABLE IF EXISTS t_pr_prewhere_scalar;
CREATE TABLE t_pr_prewhere_scalar (id UInt64, tenant UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_prewhere_scalar SELECT number, number % 8 FROM numbers(1000);

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_prefer_local_replica = 0, enable_scalar_subquery_optimization = 1;
-- Keeps `__getScalar` a function node in the shipped DAGs.
SET query_plan_remove_unused_columns = 0;

SELECT count() FROM t_pr_prewhere_scalar PREWHERE has((SELECT [2, 3]), tenant);
SELECT count() FROM t_pr_prewhere_scalar PREWHERE has((SELECT [2, 3]), tenant) WHERE has((SELECT [3]), tenant);

DROP TABLE t_pr_prewhere_scalar;
