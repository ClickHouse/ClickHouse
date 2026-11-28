-- An aggregation whose keys are the first columns of the primary key of the table. With
-- `distributed_plan_use_primary_key_range_layout` each node reads a range of the key values (`RangePartitionedRead`),
-- so the rows of one group are on one node and each node produces final results with no exchange.
-- A join on the key below the aggregation is divided at the same borders, in the same fragment.
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

DROP TABLE IF EXISTS t_range_agg_events;
DROP TABLE IF EXISTS t_range_agg_groups;
DROP TABLE IF EXISTS t_range_agg_details;

-- The ranges come from the marks of the primary index, so the tables pin their granularity.
CREATE TABLE t_range_agg_events (key UInt64, seq UInt32, value UInt64)
ENGINE = MergeTree ORDER BY (key, seq) SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_agg_groups (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 64, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_agg_details (id UInt64, weight UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '', index_granularity = 256, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_range_agg_events;
SYSTEM STOP MERGES t_range_agg_groups;
SYSTEM STOP MERGES t_range_agg_details;

INSERT INTO t_range_agg_events SELECT number % 4000, number, number FROM numbers(20000);
INSERT INTO t_range_agg_events SELECT number % 4000 + 2000, number + 100000, number FROM numbers(12000);
INSERT INTO t_range_agg_events SELECT 3000, number + 200000, number FROM numbers(1000);
-- The groups have only even keys, so the rows of the events with no match in a LEFT join are in all ranges.
INSERT INTO t_range_agg_groups SELECT number * 2, 'a' FROM numbers(3000);
INSERT INTO t_range_agg_groups SELECT number * 2 + 2500, 'b' FROM numbers(1500);
INSERT INTO t_range_agg_details SELECT number + 500, number % 7 FROM numbers(4000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_range_agg_events": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_agg_groups": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_agg_details": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"id": 100000000}}}';
SET distributed_plan_use_primary_key_range_layout = 1;

SELECT '-- 1. GROUP BY the first key column';
EXPLAIN SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key);
SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key);
SELECT count(), sum(c), sum(s) FROM (SELECT key, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key)
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. GROUP BY two key columns: the ranges use the two columns';
EXPLAIN SELECT count(), sum(c), sum(s) FROM (SELECT key, seq, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key, seq);
SELECT count(), sum(c), sum(s) FROM (SELECT key, seq, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key, seq);
SELECT count(), sum(c), sum(s) FROM (SELECT key, seq, count() AS c, sum(value) AS s FROM t_range_agg_events GROUP BY key, seq)
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. GROUP BY a column that is not the first key column: no ranges';
EXPLAIN SELECT count(), sum(c) FROM (SELECT seq, count() AS c FROM t_range_agg_events GROUP BY seq);
SELECT count(), sum(c) FROM (SELECT seq, count() AS c FROM t_range_agg_events GROUP BY seq);
SELECT count(), sum(c) FROM (SELECT seq, count() AS c FROM t_range_agg_events GROUP BY seq)
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. a join on the key and GROUP BY the key';
EXPLAIN SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 5. two joins and GROUP BY the key of the last table';
EXPLAIN SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key INNER JOIN t_range_agg_details AS d ON g.key = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key INNER JOIN t_range_agg_details AS d ON g.key = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_range_agg_events AS e INNER JOIN t_range_agg_groups AS g ON e.key = g.key INNER JOIN t_range_agg_details AS d ON g.key = d.id GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 6. LEFT join and GROUP BY the key of the right table: the rows with no match are one group on all nodes';
EXPLAIN SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 7. LEFT join and GROUP BY the key of the left table';
EXPLAIN SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_range_agg_events AS e LEFT JOIN t_range_agg_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 8. a broadcast join on another column keeps the rows of the left table on their nodes';
EXPLAIN SELECT count(), sum(c), sum(w) FROM
    (SELECT e.key AS k, count() AS c, sum(d.weight) AS w FROM t_range_agg_events AS e INNER JOIN t_range_agg_details AS d ON e.seq = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT e.key AS k, count() AS c, sum(d.weight) AS w FROM t_range_agg_events AS e INNER JOIN t_range_agg_details AS d ON e.seq = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT e.key AS k, count() AS c, sum(d.weight) AS w FROM t_range_agg_events AS e INNER JOIN t_range_agg_details AS d ON e.seq = d.id GROUP BY k)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_range_agg_events;
DROP TABLE t_range_agg_groups;
DROP TABLE t_range_agg_details;
