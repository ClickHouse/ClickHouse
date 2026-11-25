-- The statistics of a step in the cost-based distributed optimizer describe the rows and the columns that the step
-- emits. The row width is the sum of the sizes of the output columns. `UNION ALL` has the rows of all its inputs.
-- `ROLLUP`, `CUBE` and `GROUPING SETS` have the rows of all their sets of groups. A step that changes a column
-- does not keep the statistics of that column.
-- `EXPLAIN estimates = 1` shows the rows and the cost of each step with its inputs. The row width shows in the
-- cost of a window step: for each row it pays 1 unit and the bytes of its output row, which has the result of
-- the window function (8 bytes) as one more column. This cost is the difference to the line below.

DROP TABLE IF EXISTS t_stats_columns;
DROP TABLE IF EXISTS t_stats_columns_small;
DROP TABLE IF EXISTS t_stats_columns_row_hint;

-- Fixed granularity and serialization: the cost of a read comes from its marks and bytes.
CREATE TABLE t_stats_columns (k UInt32, x UInt64, s String, a Array(UInt64)) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_stats_columns_small (k UInt32, x UInt64, s String, a Array(UInt64)) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_stats_columns_row_hint (k UInt32, s String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
-- a merge between planning and the read would invalidate the planned part names
SYSTEM STOP MERGES t_stats_columns;
SYSTEM STOP MERGES t_stats_columns_small;
SYSTEM STOP MERGES t_stats_columns_row_hint;
INSERT INTO t_stats_columns SELECT number % 10, number, toString(number), [number, number + 1] FROM numbers(1000);
INSERT INTO t_stats_columns_small SELECT number % 10, number, toString(number), [number] FROM numbers(100);
INSERT INTO t_stats_columns_row_hint SELECT number % 10, toString(number) FROM numbers(100);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET distributed_aggregation_memory_efficient = 1;
SET cascades_aggregation_pushdown = 0;
SET optimize_aggregation_in_order = 0;
SET param__internal_cascades_cluster_node_count = 8;
-- The tables stand in for large ones. The first two have a size for each column: 2 bytes for `k`, 4 for `x` and 64
-- for `a`; `s` has 32 bytes in the first table and 8 in the second. The third one has a size for the rows that a
-- read emits, 34 bytes: the columns that the read emits share it by the sizes of their types, so `k` and `s`
-- together have 2 and 32 bytes, and `s` alone has 34. It has no distinct counts.
SET param__internal_join_table_stat_hints = '{"t_stats_columns": {"cardinality": 10000000, "distinct_keys": {"k": 1000, "x": 1000000}, "column_bytes": {"k": 2, "x": 4, "s": 32, "a": 64}}, "t_stats_columns_small": {"cardinality": 1000, "distinct_keys": {"k": 10, "x": 1000}, "column_bytes": {"k": 2, "x": 4, "s": 8, "a": 64}}, "t_stats_columns_row_hint": {"cardinality": 1000, "avg_row_bytes": 34}}';
-- The network is very expensive, so that the optimizer adds no exchange that a plan can do without.
SET param__internal_cascades_cost_config = '{"network_weight": 1000000}';

SELECT '-- UNION ALL has the rows and the distinct values of both inputs: the small input first';
EXPLAIN estimates = 1 SELECT x, count() FROM (SELECT x FROM t_stats_columns_small UNION ALL SELECT x FROM t_stats_columns) GROUP BY x;
SELECT '-- UNION ALL: the large input first';
EXPLAIN estimates = 1 SELECT x, count() FROM (SELECT x FROM t_stats_columns UNION ALL SELECT x FROM t_stats_columns_small) GROUP BY x;
SELECT '-- an input of UNION ALL without a distinct count adds its rows to the distinct count: 10 + 1000 groups';
EXPLAIN estimates = 1 SELECT k, count() FROM (SELECT k FROM t_stats_columns_small UNION ALL SELECT k FROM t_stats_columns_row_hint) GROUP BY k;
SELECT '-- UNION ALL has the average size of a column over the rows of its inputs: 1 + (8 + 34) / 2 + 8 for each row';
EXPLAIN estimates = 1 SELECT s, count() OVER () AS c FROM (SELECT s FROM t_stats_columns_small UNION ALL SELECT s FROM t_stats_columns_row_hint);
SELECT '-- UNION ALL: the same size with the inputs in the other order';
EXPLAIN estimates = 1 SELECT s, count() OVER () AS c FROM (SELECT s FROM t_stats_columns_row_hint UNION ALL SELECT s FROM t_stats_columns_small);

SELECT '-- ROLLUP has a set of groups for each prefix of the keys: 1000 + 10 + 1 rows';
EXPLAIN estimates = 1 SELECT k, x, count() FROM t_stats_columns_small GROUP BY k, x WITH ROLLUP ORDER BY k, x;
SELECT '-- CUBE has a set of groups for each subset of the keys: 1000 + 1000 + 10 + 1 rows';
EXPLAIN estimates = 1 SELECT k, x, count() FROM t_stats_columns_small GROUP BY k, x WITH CUBE ORDER BY k, x;
SELECT '-- GROUPING SETS: 10 + 1000 rows';
EXPLAIN estimates = 1 SELECT k, x, count() FROM t_stats_columns_small GROUP BY GROUPING SETS ((k), (x)) ORDER BY k, x;
SELECT '-- a key of ROLLUP has one more value, in the rows of the sets without the key: 10 + 1 groups by k';
EXPLAIN estimates = 1 SELECT k, count() FROM (SELECT k, x, count() FROM t_stats_columns_small GROUP BY k, x WITH ROLLUP) GROUP BY k;

SELECT '-- a window step adds its result column to the row width: 1 + 4 + 8 for each of the 1000 rows';
EXPLAIN estimates = 1 SELECT x, count() OVER () AS c FROM t_stats_columns_small;
SELECT '-- an expression that makes a string from a number emits rows with the default size of a string: 1 + 64 + 8';
EXPLAIN estimates = 1 SELECT t, count() OVER () AS c FROM (SELECT toString(x) AS t FROM t_stats_columns_small);
SELECT '-- an expression above a sort passes the columns that it does not read, and they keep their sizes: 1 + 2 + 8 + 8 + 8';
EXPLAIN estimates = 1 SELECT s, t, count() OVER (PARTITION BY k) AS c FROM (SELECT k, s, x + 1 AS t FROM t_stats_columns_small)
SETTINGS query_plan_execute_functions_after_sorting = 1;
SELECT '-- a filter over the read drops its column and passes the size of the other column: 1 + 32 + 8';
EXPLAIN estimates = 1 SELECT s, count() OVER () AS c FROM t_stats_columns_row_hint WHERE k % 3 = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT '-- a filter over the read reads only x; k passes it and keeps its distinct count: 10 groups';
EXPLAIN estimates = 1 SELECT k, max(z), count() FROM (SELECT k, x * 2 AS z, x FROM t_stats_columns_small) WHERE x > 5 GROUP BY k
SETTINGS optimize_move_to_prewhere = 0;
SELECT '-- ARRAY JOIN makes a column of the elements, which does not keep the size of the array: 1 + 2 + 8 + 8';
EXPLAIN estimates = 1 SELECT k, a, count() OVER () AS c FROM t_stats_columns_small ARRAY JOIN a;

DROP TABLE t_stats_columns;
DROP TABLE t_stats_columns_small;
DROP TABLE t_stats_columns_row_hint;
