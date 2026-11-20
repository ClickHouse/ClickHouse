-- The right-hand side of `IN` in `parallel_replicas_custom_key` may be a tuple field of a lambda argument.
-- It is not a table, so the custom key must keep working.

DROP TABLE IF EXISTS t_custom_key_in_lambda_field;

CREATE TABLE t_custom_key_in_lambda_field (a UInt64, arrs Array(Tuple(v Array(UInt64)))) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_custom_key_in_lambda_field VALUES (1, [([1, 2],)]), (2, [([2, 3],)]), (4, [([1],)]);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET serialize_query_plan = 0;

-- The replicas return partial counts, so aggregate them on top.
SELECT sum(c) FROM (SELECT count() AS c FROM t_custom_key_in_lambda_field)
SETTINGS parallel_replicas_custom_key = 'arrayCount(z -> a IN z.v, arrs)', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE t_custom_key_in_lambda_field;
