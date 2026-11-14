-- When a `GROUP BY` key has no distinct count, the group count is a default value. The cost-based
-- distributed optimizer then aggregates partially on the nodes that hold the rows and sends the states.
-- It does not add an exchange of the input rows below the aggregation. An input that is already
-- distributed by a key, for example the output of a shuffle join, is aggregated in one step as before.

DROP TABLE IF EXISTS t_points;
DROP TABLE IF EXISTS t_owners;
-- The rows of one group are in different granules, so different nodes get states of the same group.
CREATE TABLE t_points (x UInt32, y UInt32, d Date) ENGINE = MergeTree ORDER BY d
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_owners (x UInt32, owner UInt32) ENGINE = MergeTree ORDER BY x
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_points;
SYSTEM STOP MERGES t_owners;
INSERT INTO t_points SELECT number % 100, intDiv(number, 100) % 100, toDate('2026-03-01') + number % 30 FROM numbers(100000);
INSERT INTO t_owners SELECT number, number % 7 FROM numbers(1000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET cascades_aggregation_pushdown = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 8;
-- The table stands in for a large one. Its columns have no distinct counts.
SET param__internal_join_table_stat_hints = '{"t_points": {"cardinality": 2000000000, "avg_row_bytes": 10}}';

SELECT '-- 1. keys without distinct counts: partial aggregation, then a shuffle of the states';
EXPLAIN
SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
FROM t_points GROUP BY x, y HAVING count() = 10;

SELECT '-- 2. the plan of case 1 gives the same result as the plan for one server';
SELECT count(), sum(cityHash64(x, y, days)) FROM
(
    SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
    FROM t_points GROUP BY x, y HAVING count() = 10
) SETTINGS distributed_plan_fallback_to_local_execution = 0;
SELECT count(), sum(cityHash64(x, y, days)) FROM
(
    SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
    FROM t_points GROUP BY x, y HAVING count() = 10
) SETTINGS make_distributed_plan = 0;

SELECT '-- 3. one key has a distinct count that shows about one row for each group: the costs select a shuffle of the input rows';
EXPLAIN
SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
FROM t_points GROUP BY x, y HAVING count() = 10
SETTINGS param__internal_join_table_stat_hints = '{"t_points": {"cardinality": 2000000000, "avg_row_bytes": 10, "distinct_keys": {"x": 2000000000}}}';

SELECT '-- 4. distributed_plan_force_shuffle_aggregation: shuffle of the input rows';
EXPLAIN
SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
FROM t_points GROUP BY x, y HAVING count() = 10
SETTINGS distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- 5. no merge of the states on each node (distributed_plan_partial_aggregation_before_shuffle = 0): shuffle of the input rows';
EXPLAIN
SELECT x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
FROM t_points GROUP BY x, y HAVING count() = 10
SETTINGS distributed_plan_partial_aggregation_before_shuffle = 0;

SELECT '-- 6. the shuffle join distributes its output by x: one aggregation step in each bucket, no exchange between the join and the aggregation';
EXPLAIN
SELECT t_points.x, y, countResample(1, 31, 1)(toDayOfMonth(d)) AS days
FROM t_points JOIN t_owners ON t_points.x = t_owners.x GROUP BY t_points.x, y HAVING count() = 10
SETTINGS param__internal_join_table_stat_hints = '{"t_points": {"cardinality": 2000000000, "avg_row_bytes": 10, "distinct_keys": {"x": 100000000}}, "t_owners": {"cardinality": 1000000000, "avg_row_bytes": 8, "distinct_keys": {"x": 100000000}}}';

SELECT '-- 7. a small table is read on one node: one aggregation step, no exchange';
EXPLAIN
SELECT owner, count() FROM t_owners GROUP BY owner;

DROP TABLE t_points;
DROP TABLE t_owners;
