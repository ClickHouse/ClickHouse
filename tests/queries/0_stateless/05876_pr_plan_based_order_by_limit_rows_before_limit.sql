-- `rows_before_limit_at_least` of `ORDER BY ... LIMIT` under plan-based parallel replicas counts the rows of the
-- local replica too: its top-N is a shard limit.

DROP TABLE IF EXISTS t_pr_rows_before_limit;
CREATE TABLE t_pr_rows_before_limit (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_pr_rows_before_limit SELECT number FROM numbers(20000);

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET exact_rows_before_limit = 1, output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}${rows_before_limit:Raw}\n';

SELECT '' FROM t_pr_rows_before_limit WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template
SETTINGS parallel_replicas_local_plan = 0;
SELECT '' FROM t_pr_rows_before_limit WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template
SETTINGS parallel_replicas_local_plan = 1;
SELECT '' FROM t_pr_rows_before_limit WHERE id < 10000 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template
SETTINGS parallel_replicas_local_plan = 1;
SELECT '' FROM t_pr_rows_before_limit WHERE id < 10000 ORDER BY id DESC LIMIT 1 OFFSET 3 FORMAT Template
SETTINGS parallel_replicas_local_plan = 1;

DROP TABLE t_pr_rows_before_limit;
