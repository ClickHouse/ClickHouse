DROP TABLE IF EXISTS t_autopr_identical_subqueries;

CREATE TABLE t_autopr_identical_subqueries (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_autopr_identical_subqueries SELECT number FROM numbers(10000);

SET max_threads = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 0;

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_for_queries_with_multiple_tables = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

-- The identical subqueries have the same boundary hash. The first match is the left
-- `Aggregating` step, but a `RIGHT JOIN` coordinates the read in the right subquery.
SET automatic_parallel_replicas_mode = 2;
SELECT sum(a.x + b.x)
FROM (SELECT sum(v) AS x FROM t_autopr_identical_subqueries) AS a
RIGHT JOIN (SELECT sum(v) AS x FROM t_autopr_identical_subqueries) AS b USING (x);

-- The index-analysis transplant pairs reads by table-expression identity, so it would install
-- the right read's analysis, not the left read's analysis used for the decision, violating
-- `reads->in_replicas_plan->getAnalyzedResult() == analysis` in `decideAtBoundary`. The read
-- the decision stands on must have the same identity as the coordinated one, so the query
-- keeps the single-node plan.
SET automatic_parallel_replicas_mode = 1;
SELECT sum(a.x + b.x)
FROM (SELECT sum(v) AS x FROM t_autopr_identical_subqueries) AS a
RIGHT JOIN (SELECT sum(v) AS x FROM t_autopr_identical_subqueries) AS b USING (x)
SETTINGS log_comment = '05402_mode_1';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AutoParallelReplicasApplied'] AS applied,
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0 AS plans_not_comparable
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND event_date >= yesterday() AND log_comment = '05402_mode_1'
FORMAT TSVWithNames;

DROP TABLE t_autopr_identical_subqueries;
