-- With `cascades_distinct_aggregation_split`, a `count(DISTINCT x)` without `GROUP BY` that has `count`, `sum`,
-- `min`, `max` or `avg` beside it is also computed over an aggregation by `x`. That aggregation computes a state
-- of each other aggregate for each value of `x`, and the step above it counts the values and merges the states.

DROP TABLE IF EXISTS t_distinct_mixed;
DROP TABLE IF EXISTS t_distinct_mixed_empty;

-- Fixed granularity and serialization: case 1 shows the granules, and the cost of a read comes from its marks and bytes.
CREATE TABLE t_distinct_mixed (k UInt32, x UInt64, n Nullable(Int32), y UInt64, m Nullable(Int64), f Float64, s String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_distinct_mixed_empty AS t_distinct_mixed;
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_distinct_mixed;
-- The two parts have the same values of `x` and `n`, so different nodes get the same values.
-- `f` has whole numbers only, so its sum does not depend on the order of the additions.
INSERT INTO t_distinct_mixed SELECT number % 10, number % 500, if(number % 7 = 0, NULL, number % 300), number, if(number % 3 = 0, NULL, number % 1000), number % 100, toString(number % 200) FROM numbers(5000);
INSERT INTO t_distinct_mixed SELECT number % 10, number % 700, if(number % 5 = 0, NULL, number % 400), number + 5000, if(number % 4 = 0, NULL, number % 900), number % 150, toString(number % 300) FROM numbers(5000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
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
-- The tables stand in for large ones: 1 billion rows, 100 million distinct values of `x` and of `n`.
SET param__internal_join_table_stat_hints = '{"t_distinct_mixed": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 100000000, "n": 100000000}}, "t_distinct_mixed_empty": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 100000000, "n": 100000000}}}';


SELECT '-- 1. a Nullable argument between two other aggregates: their states for each value of n, then count(n) and the merges';
EXPLAIN actions = 1 SELECT sum(y), count(DISTINCT n), count() FROM t_distinct_mixed;

SELECT '-- 2. queries that the rule accepts: the split';
SELECT '-- sum, count, avg';
EXPLAIN SELECT count(DISTINCT x), sum(y), count(), avg(y) FROM t_distinct_mixed;
SELECT '-- min, max, a sum of floats';
EXPLAIN SELECT min(y), count(DISTINCT x), max(y), sum(f) FROM t_distinct_mixed;
SELECT '-- -If';
EXPLAIN SELECT count(DISTINCT x), sumIf(y, k > 3), countIf(k = 1), avgIf(y, k < 5) FROM t_distinct_mixed;
SELECT '-- -If without rows';
EXPLAIN SELECT count(DISTINCT x), minIf(y, k = 2), maxIf(m, k > 100) FROM t_distinct_mixed;
SELECT '-- Nullable arguments';
EXPLAIN SELECT count(DISTINCT x), sum(m), count(m), avg(m) FROM t_distinct_mixed;
SELECT '-- min, max of Nullable and float';
EXPLAIN SELECT count(DISTINCT x), min(m), max(f) FROM t_distinct_mixed;
SELECT '-- Nullable argument of uniqExact';
EXPLAIN SELECT count(DISTINCT n), sum(y), count(), countIf(n IS NULL) FROM t_distinct_mixed;
SELECT '-- renamed columns, an expression';
EXPLAIN SELECT count(DISTINCT z), sum(w + 1) FROM (SELECT x AS z, y AS w FROM t_distinct_mixed);
SELECT '-- only the NULL row';
EXPLAIN SELECT count(DISTINCT n), sum(y), count() FROM t_distinct_mixed WHERE n IS NULL SETTINGS optimize_move_to_prewhere = 0;
SELECT '-- no rows';
EXPLAIN SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed WHERE y % 3 = 5 SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- 3. other aggregates that the rule does not accept: the states of uniqExact go to one node';
-- not `count`, `sum`, `min`, `max` or `avg`
SELECT '-- uniq';
EXPLAIN SELECT count(DISTINCT x), uniq(y) FROM t_distinct_mixed;
SELECT '-- any';
EXPLAIN SELECT count(DISTINCT x), any(y) FROM t_distinct_mixed;
SELECT '-- -OrNull';
EXPLAIN SELECT count(DISTINCT x), sumOrNull(y) FROM t_distinct_mixed;
SELECT '-- -State';
EXPLAIN SELECT count(DISTINCT x), sumState(y) FROM t_distinct_mixed;
-- for an argument that is only NULL the function is `nothing`; with such a condition its `-State` form has no state
SELECT '-- only NULL';
EXPLAIN SELECT count(DISTINCT x), count(NULL), sum(NULL) FROM t_distinct_mixed;
SELECT '-- -If of NULL';
EXPLAIN SELECT count(DISTINCT x), sumIf(y, NULL), countIf(NULL) FROM t_distinct_mixed;
-- a state that has no fixed size
SELECT '-- max of String';
EXPLAIN SELECT count(DISTINCT x), max(s) FROM t_distinct_mixed;
SELECT '-- second uniqExact';
EXPLAIN SELECT count(DISTINCT x), count(DISTINCT n) FROM t_distinct_mixed;

SELECT '-- 4. two nodes, a narrow table, each row has its own value: the hash sets of uniqExact on one node cost more than the shuffle';
SET param__internal_join_table_stat_hints = '{"t_distinct_mixed": {"cardinality": 100000000, "avg_row_bytes": 10, "distinct_keys": {"x": 100000000}}}';
SET param__internal_cascades_cluster_node_count = 2;
EXPLAIN SELECT count(DISTINCT x), sum(y) FROM t_distinct_mixed;
SET param__internal_cascades_cluster_node_count = 8;
SET param__internal_join_table_stat_hints = '{"t_distinct_mixed": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 100000000, "n": 100000000}}, "t_distinct_mixed_empty": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"x": 100000000, "n": 100000000}}}';

SELECT '-- 5. the results of the queries of case 2 and of five other queries';
SELECT count(DISTINCT x), sum(y), count(), avg(y) FROM t_distinct_mixed;
SELECT min(y), count(DISTINCT x), max(y), sum(f) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sumIf(y, k > 3), countIf(k = 1), avgIf(y, k < 5) FROM t_distinct_mixed;
SELECT count(DISTINCT x), minIf(y, k = 2), maxIf(m, k > 100) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sum(m), count(m), avg(m) FROM t_distinct_mixed;
SELECT count(DISTINCT x), min(m), max(f) FROM t_distinct_mixed;
SELECT count(DISTINCT n), sum(y), count(), countIf(n IS NULL) FROM t_distinct_mixed;
SELECT count(DISTINCT z), sum(w + 1) FROM (SELECT x AS z, y AS w FROM t_distinct_mixed);
SELECT count(DISTINCT n), sum(y), count() FROM t_distinct_mixed WHERE n IS NULL SETTINGS optimize_move_to_prewhere = 0;
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed WHERE y % 3 = 5 SETTINGS optimize_move_to_prewhere = 0;
-- no rows with `empty_result_for_aggregation_by_empty_set`; an empty table; aggregates that the rule does not accept
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed WHERE y % 3 = 5 SETTINGS optimize_move_to_prewhere = 0, empty_result_for_aggregation_by_empty_set = 1;
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed_empty;
SELECT count(DISTINCT x), uniq(y), sumOrNull(y), max(s), count(DISTINCT n) FROM t_distinct_mixed;
SELECT count(DISTINCT x), finalizeAggregation(sumState(y)), count(NULL), sum(NULL), sumIf(y, NULL), countIf(NULL) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sumMerge(state) FROM (SELECT x, sumState(y) AS state FROM t_distinct_mixed GROUP BY x, k);

SELECT '-- 6. the same with the setting off';
SET cascades_distinct_aggregation_split = 0;
SELECT count(DISTINCT x), sum(y), count(), avg(y) FROM t_distinct_mixed;
SELECT min(y), count(DISTINCT x), max(y), sum(f) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sumIf(y, k > 3), countIf(k = 1), avgIf(y, k < 5) FROM t_distinct_mixed;
SELECT count(DISTINCT x), minIf(y, k = 2), maxIf(m, k > 100) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sum(m), count(m), avg(m) FROM t_distinct_mixed;
SELECT count(DISTINCT x), min(m), max(f) FROM t_distinct_mixed;
SELECT count(DISTINCT n), sum(y), count(), countIf(n IS NULL) FROM t_distinct_mixed;
SELECT count(DISTINCT z), sum(w + 1) FROM (SELECT x AS z, y AS w FROM t_distinct_mixed);
SELECT count(DISTINCT n), sum(y), count() FROM t_distinct_mixed WHERE n IS NULL SETTINGS optimize_move_to_prewhere = 0;
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed WHERE y % 3 = 5 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed WHERE y % 3 = 5 SETTINGS optimize_move_to_prewhere = 0, empty_result_for_aggregation_by_empty_set = 1;
SELECT count(DISTINCT x), sum(y), count(), min(m) FROM t_distinct_mixed_empty;
SELECT count(DISTINCT x), uniq(y), sumOrNull(y), max(s), count(DISTINCT n) FROM t_distinct_mixed;
SELECT count(DISTINCT x), finalizeAggregation(sumState(y)), count(NULL), sum(NULL), sumIf(y, NULL), countIf(NULL) FROM t_distinct_mixed;
SELECT count(DISTINCT x), sumMerge(state) FROM (SELECT x, sumState(y) AS state FROM t_distinct_mixed GROUP BY x, k);
SET cascades_distinct_aggregation_split = 1;

SELECT '-- 7. the names and the types of the result columns are those of the query';
SELECT count(DISTINCT n), sum(y) AS total, avg(m), min(m) FROM t_distinct_mixed FORMAT TSVWithNamesAndTypes;

DROP TABLE t_distinct_mixed;
DROP TABLE t_distinct_mixed_empty;
