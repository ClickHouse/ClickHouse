-- The rows at the borders of the key ranges of `distributed_plan_use_primary_key_range_layout`. A granule that has
-- keys on the two sides of a border is read by two nodes, and each node must keep only the rows of its range. This
-- test has many such granules: a small granularity, parts with keys in common, small parts, and one key with many
-- rows. A row that two nodes return, or that no node returns, changes the counts.
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

DROP TABLE IF EXISTS t_range_borders_left;
DROP TABLE IF EXISTS t_range_borders_right;
DROP TABLE IF EXISTS t_range_borders_few;

CREATE TABLE t_range_borders_left (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 8, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_borders_right (key UInt64, weight UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 16, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_borders_few (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 8, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_range_borders_left;
SYSTEM STOP MERGES t_range_borders_right;
SYSTEM STOP MERGES t_range_borders_few;

-- Left: two parts with keys in common, the key 700 with 400 rows (50 granules), and four small parts.
INSERT INTO t_range_borders_left SELECT number % 1000, number FROM numbers(3000);
INSERT INTO t_range_borders_left SELECT number % 1000 + 500, number FROM numbers(2000);
INSERT INTO t_range_borders_left SELECT 700, number FROM numbers(400);
INSERT INTO t_range_borders_left SELECT number * 300, number FROM numbers(5);
INSERT INTO t_range_borders_left SELECT 699 + number, number FROM numbers(3);
INSERT INTO t_range_borders_left SELECT 1499, number FROM numbers(1);
INSERT INTO t_range_borders_left SELECT 0, number FROM numbers(9);
-- Right: each key one or two times.
INSERT INTO t_range_borders_right SELECT number, number FROM numbers(1200);
INSERT INTO t_range_borders_right SELECT number * 2 + 400, number FROM numbers(600);
INSERT INTO t_range_borders_right SELECT 700, number FROM numbers(3);
-- Two keys only: three of the four ranges are empty.
INSERT INTO t_range_borders_few SELECT number % 2, number FROM numbers(200);
INSERT INTO t_range_borders_few SELECT number % 2, number FROM numbers(100);

SET param__internal_join_table_stat_hints = '{"t_range_borders_left": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_borders_right": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_borders_few": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}}';
-- The ranges are not equal here: one key has a large part of the rows. The network weight makes each plan with an
-- exchange of table rows more expensive than the plan that reads by ranges.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":1000,"sequential_weight":32}';
SET distributed_plan_use_primary_key_range_layout = 1;

SELECT '-- 1. inner join';
EXPLAIN SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l INNER JOIN t_range_borders_right AS r ON l.key = r.key;
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l INNER JOIN t_range_borders_right AS r ON l.key = r.key;
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l INNER JOIN t_range_borders_right AS r ON l.key = r.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. full join';
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l FULL JOIN t_range_borders_right AS r ON l.key = r.key;
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l FULL JOIN t_range_borders_right AS r ON l.key = r.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. GROUP BY the key';
EXPLAIN SELECT count(), sum(c), max(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_left GROUP BY key);
SELECT count(), sum(c), max(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_left GROUP BY key);
SELECT count(), sum(c), max(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_left GROUP BY key)
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. the rows of each key, in the two plans';
SELECT sum(cityHash64(key, c, s)) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_left GROUP BY key);
SELECT sum(cityHash64(key, c, s)) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_left GROUP BY key)
SETTINGS make_distributed_plan = 0;

SELECT '-- 5. small blocks';
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l INNER JOIN t_range_borders_right AS r ON l.key = r.key
SETTINGS max_block_size = 5, max_threads = 3, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;
SELECT count(), sum(l.value), sum(r.weight) FROM t_range_borders_left AS l INNER JOIN t_range_borders_right AS r ON l.key = r.key
SETTINGS max_block_size = 5, max_threads = 3, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1, make_distributed_plan = 0;

SELECT '-- 6. fewer key values than nodes';
EXPLAIN SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_few GROUP BY key);
SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_few GROUP BY key);
SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_borders_few GROUP BY key)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_range_borders_left;
DROP TABLE t_range_borders_right;
DROP TABLE t_range_borders_few;
