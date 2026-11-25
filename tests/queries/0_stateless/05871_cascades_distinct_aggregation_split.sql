-- With `cascades_distinct_aggregation_split` the cost-based distributed optimizer has one more plan for a
-- `count(DISTINCT x)` without `GROUP BY`: the count of the rows of an aggregation by `x`. The plans of
-- `uniqExact` stay, and the costs select.

DROP TABLE IF EXISTS t_distinct_split;
DROP TABLE IF EXISTS t_distinct_split_narrow;
DROP TABLE IF EXISTS t_distinct_split_measured;

-- Fixed granularity and serialization: case 3 shows the granules, and the cost of a read comes from its marks and bytes.
CREATE TABLE t_distinct_split (k UInt32, x UInt64, n Nullable(Int32), s String, f Float64, y UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_distinct_split_narrow (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_distinct_split_measured (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_distinct_split;
SYSTEM STOP MERGES t_distinct_split_narrow;
SYSTEM STOP MERGES t_distinct_split_measured;
-- The two parts have the same values, so different nodes get the same values.
INSERT INTO t_distinct_split SELECT number % 10, number % 500, if(number % 7 = 0, NULL, number % 300), toString(number % 200), number % 100, number FROM numbers(5000);
INSERT INTO t_distinct_split SELECT number % 10, number % 700, if(number % 5 = 0, NULL, number % 400), toString(number % 300), number % 150, number FROM numbers(5000);
INSERT INTO t_distinct_split_narrow SELECT number % 10, number FROM numbers(10000);
INSERT INTO t_distinct_split_narrow SELECT number % 10, number + 5000 FROM numbers(10000);
-- Case 11 uses the distinct count that these inserts measure.
INSERT INTO t_distinct_split_measured SETTINGS materialize_statistics_on_insert = 1 SELECT number % 10, number FROM numbers(10000);
INSERT INTO t_distinct_split_measured SETTINGS materialize_statistics_on_insert = 1 SELECT number % 10, number + 5000 FROM numbers(10000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET count_distinct_implementation = 'uniqExact';
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET distributed_aggregation_memory_efficient = 1;
SET cascades_aggregation_pushdown = 0;
SET optimize_aggregation_in_order = 0;
SET cascades_distinct_aggregation_split = 1;
SET param__internal_cascades_cluster_node_count = 8;
-- The table stands in for a large one: 1 billion rows, 100 million distinct values in each column.
SET param__internal_join_table_stat_hints = '{"t_distinct_split": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 100000000, "n": 100000000, "s": 100000000, "f": 100000000, "k": 100000000}}}';

SELECT '-- 1. many distinct values: an aggregation by x on every node, a shuffle by x, then the count';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split;

SELECT '-- 2. the same query with the setting off: the states of uniqExact go to one node';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split SETTINGS cascades_distinct_aggregation_split = 0;

SELECT '-- 3. a Nullable argument: the aggregation above the shuffle is count(n), which does not count the NULL group';
EXPLAIN actions = 1 SELECT count(DISTINCT n) FROM t_distinct_split;

SELECT '-- 3b. the column of a subquery under another name: the same plan as in case 1';
EXPLAIN SELECT count(DISTINCT z) FROM (SELECT x AS z FROM t_distinct_split);

SELECT '-- 4. a filter below the aggregation: the same plan over the filter';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split WHERE y > 10 SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- 5. a limit on the rows of GROUP BY must not apply to the distinct values: no split';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split SETTINGS max_rows_to_group_by = 1000000;

SELECT '-- 6. not a uniqExact of one column without GROUP BY: no split';
EXPLAIN SELECT uniqExactIf(x, y > 10) FROM t_distinct_split;
EXPLAIN SELECT uniqExact(x, k) FROM t_distinct_split;
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split SETTINGS count_distinct_implementation = 'uniq';
-- The shuffle is forced, because the two distributed plans of an aggregation with keys are near in cost here.
EXPLAIN SELECT k, count(DISTINCT x) FROM t_distinct_split GROUP BY k SETTINGS distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- 7. uniqExact keeps a hash of a String, GROUP BY compares the strings: no split';
EXPLAIN SELECT count(DISTINCT s) FROM t_distinct_split;
SELECT '-- 7b. an aggregation in the order of the key takes 0. and -0. for one value, uniqExact does not: no split';
EXPLAIN SELECT count(DISTINCT f) FROM t_distinct_split;

SELECT '-- 8. a forced shuffle: the aggregation by x must shuffle all the rows, so the states of one million values cost less';
SET param__internal_join_table_stat_hints = '{"t_distinct_split": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 1000000}}}';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split SETTINGS distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- 9. no distinct count for the column: the default estimate (a tenth of the rows) gives the split';
SET param__internal_join_table_stat_hints = '{"t_distinct_split": {"cardinality": 1000000000, "avg_row_bytes": 40}}';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split;

SELECT '-- 10. two nodes, almost every row has its own value: uniqExact on one node costs less than a shuffle of the rows';
SET param__internal_join_table_stat_hints = '{"t_distinct_split_narrow": {"cardinality": 1000000000, "avg_row_bytes": 12, "distinct_keys": {"x": 900000000}}}';
SET param__internal_cascades_cluster_node_count = 2;
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split_narrow;
SET param__internal_cascades_cluster_node_count = 8;

SELECT '-- 11. no hint: the distinct count comes from the statistics of the table';
-- Without the overhead of an exchange this small table is read on all nodes.
SET param__internal_join_table_stat_hints = '{}';
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead": 0}';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split_measured SETTINGS use_statistics = 1, min_marks_per_part_to_load_statistics = 0;
SET param__internal_cascades_cost_config = '{}';

SELECT '-- 12. the table of case 10 on eight nodes: the split, with a shuffle of the rows';
SET param__internal_join_table_stat_hints = '{"t_distinct_split_narrow": {"cardinality": 1000000000, "avg_row_bytes": 12, "distinct_keys": {"x": 900000000}}}';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split_narrow;

SELECT '-- 13. the same when an exchange is very expensive: one node reads the table and computes uniqExact';
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead": 10000000000}';
EXPLAIN SELECT count(DISTINCT x) FROM t_distinct_split_narrow;

SELECT '-- 14. a ROLLUP finalizes the states of uniqExact, so its one row holds no values and is broadcast to the nodes of the join';
-- The network is expensive here. If the row of the ROLLUP had the price of the 1 million values of the state,
-- its broadcast would cost more than the read of the other table on one node, and the join would be on one node.
-- The join key of the other table has 10 values, so the join has many rows, and the count above it has two stages.
SET param__internal_join_table_stat_hints = '{"t_distinct_split_narrow": {"cardinality": 10000000, "avg_row_bytes": 12, "distinct_keys": {"x": 1000000}}, "t_distinct_split": {"cardinality": 214000000, "avg_row_bytes": 40, "distinct_keys": {"k": 10}}}';
SET param__internal_cascades_cost_config = '{"network_weight": 1000}';
EXPLAIN SELECT count() FROM t_distinct_split AS d INNER JOIN (SELECT uniqExact(x) AS v FROM t_distinct_split_narrow GROUP BY ALL WITH ROLLUP) AS a ON d.k = a.v % 10
SETTINGS enable_join_runtime_filters = 0, query_plan_optimize_join_order_randomize = 0;

DROP TABLE t_distinct_split;
DROP TABLE t_distinct_split_narrow;
DROP TABLE t_distinct_split_measured;
