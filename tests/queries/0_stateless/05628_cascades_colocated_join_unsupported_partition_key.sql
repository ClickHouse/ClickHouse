-- A join is co-located by a `PARTITION BY` component only when the component is made of columns, literals and
-- functions whose result depends only on the argument values. Each pair of tables in this test has partition
-- keys that look equal, but equal join keys are in different partitions. A co-located join of such a pair
-- would lose rows, so the join must have an exchange and must return the same result as on one node.

SET explain_query_plan_default = 'legacy';
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
-- The test pins full EXPLAIN outputs, so the randomized settings that shape these plans are pinned.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_remove_unused_columns = 1;
SET enable_join_transitive_predicates = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET distributed_plan_use_partition_key_layout = 1;

DROP TABLE IF EXISTS t_layout_hash_left;
DROP TABLE IF EXISTS t_layout_hash_right;
DROP TABLE IF EXISTS t_layout_set_left;
DROP TABLE IF EXISTS t_layout_set_right;
DROP TABLE IF EXISTS t_layout_lambda_left;
DROP TABLE IF EXISTS t_layout_lambda_right;

-- The cost of a read by partition value counts marks, so the tables pin their granularity.
-- A supported key: the same tables as below, to show that the co-located plan is the cheapest one for them.
CREATE TABLE t_layout_hash_left (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(k) % 8 ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_layout_hash_right (k UInt64, w UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(k) % 8 ORDER BY w SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Different sets in `IN`.
CREATE TABLE t_layout_set_left (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY (k % 4) * 2 + if(k % 16 IN (0, 1, 2, 3, 4, 5, 6, 7), 1, 0) ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_layout_set_right (k UInt64, w UInt64)
ENGINE = MergeTree PARTITION BY (k % 4) * 2 + if(k % 16 IN (4, 5, 6, 7, 8, 9, 10, 11), 1, 0) ORDER BY w SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Different lambda bodies with the same argument and result types.
CREATE TABLE t_layout_lambda_left (k Array(UInt64), v UInt64)
ENGINE = MergeTree PARTITION BY arraySum(arrayMap(x -> x + 1, k)) % 8 ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_layout_lambda_right (k Array(UInt64), w UInt64)
ENGINE = MergeTree PARTITION BY arraySum(arrayMap(x -> bitXor(x, 1), k)) % 8 ORDER BY w SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_layout_hash_left;
SYSTEM STOP MERGES t_layout_hash_right;
SYSTEM STOP MERGES t_layout_set_left;
SYSTEM STOP MERGES t_layout_set_right;
SYSTEM STOP MERGES t_layout_lambda_left;
SYSTEM STOP MERGES t_layout_lambda_right;

-- 2000 keys in each right table, each key two times in the left table: every join returns 4000 rows.
INSERT INTO t_layout_hash_left SELECT number % 2000, number FROM numbers(4000);
INSERT INTO t_layout_hash_right SELECT number, number % 7 FROM numbers(2000);
INSERT INTO t_layout_set_left SELECT number % 2000, number FROM numbers(4000);
INSERT INTO t_layout_set_right SELECT number, number % 7 FROM numbers(2000);
INSERT INTO t_layout_lambda_left SELECT [number % 2000], number FROM numbers(4000);
INSERT INTO t_layout_lambda_right SELECT [number], number % 7 FROM numbers(2000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_layout_hash_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_hash_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_set_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_set_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_lambda_left": {"cardinality": 1000000000, "avg_row_bytes": 24, "distinct_keys": {"k": 100000000}}, "t_layout_lambda_right": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"k": 100000000}}}';

SELECT '-- 1. a supported key: co-located';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_layout_hash_left AS l INNER JOIN t_layout_hash_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_hash_left AS l INNER JOIN t_layout_hash_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_hash_left AS l INNER JOIN t_layout_hash_right AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

SELECT '-- 2. different sets in IN: not co-located';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_layout_set_left AS l INNER JOIN t_layout_set_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_set_left AS l INNER JOIN t_layout_set_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_set_left AS l INNER JOIN t_layout_set_right AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

SELECT '-- 3. different lambda bodies: not co-located';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_layout_lambda_left AS l INNER JOIN t_layout_lambda_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_lambda_left AS l INNER JOIN t_layout_lambda_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_lambda_left AS l INNER JOIN t_layout_lambda_right AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

DROP TABLE t_layout_hash_left;
DROP TABLE t_layout_hash_right;
DROP TABLE t_layout_set_left;
DROP TABLE t_layout_set_right;
DROP TABLE t_layout_lambda_left;
DROP TABLE t_layout_lambda_right;
