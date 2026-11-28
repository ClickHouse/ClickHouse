-- `rows_before_limit_at_least` of `ORDER BY ... LIMIT ... WITH TIES` under plan-based parallel replicas counts the rows
-- of the local replica too: its top rows with ties are a shard limit.

DROP TABLE IF EXISTS t_pr_with_ties;
CREATE TABLE t_pr_with_ties (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_pr_with_ties SELECT number FROM numbers(20000);

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET exact_rows_before_limit = 1, output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}${rows_before_limit:Raw}\n';

SELECT '' FROM t_pr_with_ties WHERE id < 30 ORDER BY id LIMIT 1 WITH TIES FORMAT Template
SETTINGS parallel_replicas_local_plan = 1;
SELECT '' FROM t_pr_with_ties WHERE id < 30 ORDER BY id DESC LIMIT 1 WITH TIES FORMAT Template
SETTINGS parallel_replicas_local_plan = 1;

-- The rows with ties are the same as without parallel replicas.
SELECT arraySort(groupArray(k)) FROM (SELECT intDiv(id, 7) AS k FROM t_pr_with_ties ORDER BY k LIMIT 3 OFFSET 2 WITH TIES)
SETTINGS parallel_replicas_local_plan = 1;
SELECT arraySort(groupArray(k)) FROM (SELECT intDiv(id, 7) AS k FROM t_pr_with_ties ORDER BY k LIMIT 3 OFFSET 2 WITH TIES)
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_pr_with_ties;
