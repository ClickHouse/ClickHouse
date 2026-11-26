-- The statistics of a step in the cost-based distributed optimizer describe the rows that the step emits.
-- `UNION ALL` has the rows of all its inputs. `ROLLUP`, `CUBE` and `GROUPING SETS` have the rows of all their
-- sets of groups.
-- `EXPLAIN estimates = 1` shows the row estimate and the cost of each step. The test prints the row estimates
-- only: they come from the statistics alone, and a cost depends on the whole cost model.

DROP TABLE IF EXISTS t_stats_rows_large;
DROP TABLE IF EXISTS t_stats_rows_small;
DROP TABLE IF EXISTS t_stats_rows_no_counts;

CREATE TABLE t_stats_rows_large (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_stats_rows_small (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_stats_rows_no_counts (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_stats_rows_large SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_stats_rows_small SELECT number % 10, number FROM numbers(100);
INSERT INTO t_stats_rows_no_counts SELECT number % 10, number FROM numbers(100);

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
-- The tables stand in for large ones. The third one has no distinct counts.
SET param__internal_join_table_stat_hints = '{"t_stats_rows_large": {"cardinality": 10000000, "distinct_keys": {"k": 1000, "x": 1000000}}, "t_stats_rows_small": {"cardinality": 1000, "distinct_keys": {"k": 10, "x": 1000}}, "t_stats_rows_no_counts": {"cardinality": 1000}}';
-- The network is very expensive, so that every plan has the fewest exchanges that it can have.
SET param__internal_cascades_cost_config = '{"network_weight": 1000000}';

SELECT '-- UNION ALL has the rows and the distinct values of both inputs: 10001000 rows, 1001000 groups';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT x, count() FROM (SELECT x FROM t_stats_rows_small UNION ALL SELECT x FROM t_stats_rows_large) GROUP BY x);
SELECT '-- UNION ALL: the same estimates with the inputs in the other order';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT x, count() FROM (SELECT x FROM t_stats_rows_large UNION ALL SELECT x FROM t_stats_rows_small) GROUP BY x);
SELECT '-- an input of UNION ALL without a distinct count adds its rows to the distinct count: 10 + 1000 groups';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT k, count() FROM (SELECT k FROM t_stats_rows_small UNION ALL SELECT k FROM t_stats_rows_no_counts) GROUP BY k);

SELECT '-- ROLLUP has a set of groups for each prefix of the keys: 1000 + 10 + 1 rows';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT k, x, count() FROM t_stats_rows_small GROUP BY k, x WITH ROLLUP);
SELECT '-- CUBE has a set of groups for each subset of the keys: 1000 + 1000 + 10 + 1 rows';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT k, x, count() FROM t_stats_rows_small GROUP BY k, x WITH CUBE);
SELECT '-- GROUPING SETS: 10 + 1000 rows';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT k, x, count() FROM t_stats_rows_small GROUP BY GROUPING SETS ((k), (x)));
SELECT '-- a key of ROLLUP has one more value, in the rows of the sets without the key: 10 + 1 groups by k';
SELECT replaceRegexpAll(explain, ', cost: [^)]+', '') FROM (EXPLAIN estimates = 1
SELECT k, count() FROM (SELECT k, x, count() FROM t_stats_rows_small GROUP BY k, x WITH ROLLUP) GROUP BY k);

DROP TABLE t_stats_rows_large;
DROP TABLE t_stats_rows_small;
DROP TABLE t_stats_rows_no_counts;
