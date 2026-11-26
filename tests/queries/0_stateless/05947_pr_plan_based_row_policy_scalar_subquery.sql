-- A row policy's scalar subquery must reach a plan-based parallel replicas fragment as a value: the fragment
-- carries only the shard scalars.
DROP TABLE IF EXISTS t_pr_rp_scalar;
CREATE TABLE t_pr_rp_scalar (id UInt64, tenant UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_rp_scalar SELECT number, number % 8 FROM numbers(1000);

DROP ROW POLICY IF EXISTS rp_pr_scalar ON t_pr_rp_scalar;
CREATE ROW POLICY rp_pr_scalar ON t_pr_rp_scalar FOR SELECT USING has((SELECT [2, 3]), tenant) TO ALL;

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_prefer_local_replica = 0, enable_scalar_subquery_optimization = 1;
SET query_plan_remove_unused_columns = 0;

SELECT count() FROM t_pr_rp_scalar;
SELECT DISTINCT tenant FROM t_pr_rp_scalar ORDER BY tenant;

DROP ROW POLICY rp_pr_scalar ON t_pr_rp_scalar;
SELECT count() FROM t_pr_rp_scalar SETTINGS additional_table_filters = {'t_pr_rp_scalar': 'has((SELECT [3]), tenant)'};

DROP TABLE t_pr_rp_scalar;
