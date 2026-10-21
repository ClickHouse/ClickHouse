-- The right-hand side of `IN` in `parallel_replicas_custom_key` may be a lambda argument or a
-- table-qualified array column. Neither is a table, so the custom key must keep working.

DROP TABLE IF EXISTS t_custom_key_in_lambda;
DROP TABLE IF EXISTS t_custom_key_in_lambda_set;

CREATE TABLE t_custom_key_in_lambda (a UInt64, arr Array(UInt64), arrs Array(Array(UInt64))) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_custom_key_in_lambda VALUES (1, [1, 2], [[1], [2]]), (2, [2, 3], [[2], [3]]), (3, [4], [[1], [4]]);

CREATE TABLE t_custom_key_in_lambda_set (id UInt64) ENGINE = Set;
INSERT INTO t_custom_key_in_lambda_set VALUES (1);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET serialize_query_plan = 0;

-- The replicas return partial counts, so aggregate them on top.
SELECT sum(c) FROM (SELECT count() AS c FROM t_custom_key_in_lambda)
SETTINGS parallel_replicas_custom_key = 'arrayCount(z -> a IN z, arrs)', parallel_replicas_mode = 'custom_key_sampling';

SELECT sum(c) FROM (SELECT count() AS c FROM t_custom_key_in_lambda)
SETTINGS parallel_replicas_custom_key = 'a IN t_custom_key_in_lambda.arr', parallel_replicas_mode = 'custom_key_sampling';

-- A lambda argument does not hide a table with the same name outside of the lambda.
SELECT count() FROM t_custom_key_in_lambda
SETTINGS parallel_replicas_custom_key = 'arrayCount(t_custom_key_in_lambda_set -> 1, arrs) + (a IN t_custom_key_in_lambda_set)', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_custom_key_in_lambda;
DROP TABLE t_custom_key_in_lambda_set;
