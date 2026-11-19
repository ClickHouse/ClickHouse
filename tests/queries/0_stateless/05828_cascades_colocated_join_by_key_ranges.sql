-- A join of tables that are sorted by the join key. With `distributed_plan_use_primary_key_range_layout` the optimizer
-- selects ranges of the key from the primary index of all tables of the join, one range for each node. Each read then
-- gives each node the rows of its range (`RangePartitionedRead`), and each node joins them with no exchange.
-- Each query runs with the distributed plan and with the plan of one node: the results must be equal.

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
SET optimize_aggregation_in_order = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET cascades_aggregation_pushdown = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_range_join_events;
DROP TABLE IF EXISTS t_range_join_groups;
DROP TABLE IF EXISTS t_range_join_details;

-- The ranges come from the marks of the primary index, so the tables pin their granularity.
CREATE TABLE t_range_join_events (key UInt64, seq UInt32, value UInt64)
ENGINE = MergeTree ORDER BY (key, seq) SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_join_groups (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 64, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_join_details (id UInt64, weight UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '', index_granularity = 256, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_range_join_events;
SYSTEM STOP MERGES t_range_join_groups;
SYSTEM STOP MERGES t_range_join_details;

-- The parts of a table have keys in common, and the parts of different tables have different borders.
-- The key 3000 has 1000 more rows: they take several granules of one part.
INSERT INTO t_range_join_events SELECT number % 4000, number, number FROM numbers(20000);
INSERT INTO t_range_join_events SELECT number % 4000 + 2000, number + 100000, number FROM numbers(12000);
INSERT INTO t_range_join_events SELECT 3000, number + 200000, number FROM numbers(1000);
INSERT INTO t_range_join_groups SELECT number, 'a' FROM numbers(5000);
INSERT INTO t_range_join_groups SELECT number + 2500, 'b' FROM numbers(5000);
INSERT INTO t_range_join_details SELECT number + 500, number % 7 FROM numbers(4000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_range_join_events": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_join_groups": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_join_details": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"id": 100000000}}}';
SET distributed_plan_use_primary_key_range_layout = 1;

SELECT '-- 1. inner join of two tables';
EXPLAIN SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. left join';
SELECT count(), sum(e.value), countIf(g.owner = '') FROM t_range_join_events AS e LEFT JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value), countIf(g.owner = '') FROM t_range_join_events AS e LEFT JOIN t_range_join_groups AS g ON e.key = g.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. right join';
SELECT count(), sum(e.value), countIf(e.seq = 0) FROM t_range_join_events AS e RIGHT JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value), countIf(e.seq = 0) FROM t_range_join_events AS e RIGHT JOIN t_range_join_groups AS g ON e.key = g.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. full join';
EXPLAIN SELECT count(), sum(e.value), countIf(e.seq = 0) FROM t_range_join_events AS e FULL JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value), countIf(e.seq = 0) FROM t_range_join_events AS e FULL JOIN t_range_join_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value), countIf(e.seq = 0) FROM t_range_join_events AS e FULL JOIN t_range_join_groups AS g ON e.key = g.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 5. three tables: all reads use the same ranges';
EXPLAIN SELECT count(), sum(e.value), sum(d.weight)
FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key INNER JOIN t_range_join_details AS d ON g.key = d.id;
SELECT count(), sum(e.value), sum(d.weight)
FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key INNER JOIN t_range_join_details AS d ON g.key = d.id;
SELECT count(), sum(e.value), sum(d.weight)
FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key INNER JOIN t_range_join_details AS d ON g.key = d.id
SETTINGS make_distributed_plan = 0;

SELECT '-- 6. a filter on the key: the ranges divide the selected marks';
EXPLAIN SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key
WHERE e.key >= 1000 AND e.key < 5000;
SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key
WHERE e.key >= 1000 AND e.key < 5000;
SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key
WHERE e.key >= 1000 AND e.key < 5000
SETTINGS make_distributed_plan = 0;

SELECT '-- 7. the join column is not the first key column: no ranges';
EXPLAIN SELECT count() FROM t_range_join_events AS e INNER JOIN t_range_join_details AS d ON e.seq = d.id;
SELECT count() FROM t_range_join_events AS e INNER JOIN t_range_join_details AS d ON e.seq = d.id;
SELECT count() FROM t_range_join_events AS e INNER JOIN t_range_join_details AS d ON e.seq = d.id
SETTINGS make_distributed_plan = 0;

SELECT '-- 8. the setting is off';
SET distributed_plan_use_primary_key_range_layout = 0;
EXPLAIN SELECT count(), sum(e.value) FROM t_range_join_events AS e INNER JOIN t_range_join_groups AS g ON e.key = g.key;

DROP TABLE t_range_join_events;
DROP TABLE t_range_join_groups;
DROP TABLE t_range_join_details;
