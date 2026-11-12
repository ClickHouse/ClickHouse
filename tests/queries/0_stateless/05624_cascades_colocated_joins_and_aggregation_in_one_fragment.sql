-- Several joins and an aggregation on the same key, on tables that have the same expression of the key in `PARTITION BY`:
-- with `distributed_plan_use_partition_key_layout` they all run in one fragment. A co-located join declares the layout
-- on its output for the sides that it preserves, so the next join and an aggregation on the key need no exchange.

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

DROP TABLE IF EXISTS t_coloc_chain_events;
DROP TABLE IF EXISTS t_coloc_chain_groups;
DROP TABLE IF EXISTS t_coloc_chain_details;

CREATE TABLE t_coloc_chain_events (key UInt64, tag LowCardinality(String), value UInt64, day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY (tag, value) SETTINGS auto_statistics_types = '';
CREATE TABLE t_coloc_chain_groups (key UInt64, owner LowCardinality(String))
ENGINE = MergeTree PARTITION BY cityHash64(key) % 8 ORDER BY owner SETTINGS auto_statistics_types = '';
CREATE TABLE t_coloc_chain_details (id UInt64, weight UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(id) % 8 ORDER BY weight SETTINGS auto_statistics_types = '';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_chain_events;
SYSTEM STOP MERGES t_coloc_chain_groups;
SYSTEM STOP MERGES t_coloc_chain_details;

-- 2000 keys with 10 events each, in two `day` partitions. Groups have the keys 1000 .. 2999, details the keys 500 .. 2499.
INSERT INTO t_coloc_chain_events
SELECT number % 2000, ['a', 'b', 'c', 'd'][number % 4 + 1], number, toDate('2026-01-01') + intDiv(number, 2000) % 2 FROM numbers(20000);
INSERT INTO t_coloc_chain_groups SELECT number + 1000, ['x', 'y', 'z'][number % 3 + 1] FROM numbers(2000);
INSERT INTO t_coloc_chain_details SELECT number + 500, number % 7 FROM numbers(2000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_coloc_chain_events": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_coloc_chain_groups": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"key": 100000000}}, "t_coloc_chain_details": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"id": 100000000}}}';
SET distributed_plan_use_partition_key_layout = 1;

SELECT '-- 1. two joins on the key';
EXPLAIN SELECT count(), sum(e.value), sum(d.weight)
FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id;
SELECT count(), sum(e.value), sum(d.weight)
FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id;
SELECT count(), sum(e.value), sum(d.weight)
FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. a join and GROUP BY the key';
EXPLAIN SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), sum(s) FROM
    (SELECT e.key AS k, count() AS c, sum(e.value) AS s FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. two joins and GROUP BY the key of the last table';
EXPLAIN SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id GROUP BY k);
SELECT count(), sum(c), sum(w) FROM
    (SELECT d.id AS k, count() AS c, sum(d.weight) AS w
     FROM t_coloc_chain_events AS e INNER JOIN t_coloc_chain_groups AS g ON e.key = g.key INNER JOIN t_coloc_chain_details AS d ON g.key = d.id GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. LEFT join and GROUP BY the key of the right table: the rows with no match are one group on all nodes';
EXPLAIN SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT g.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- 5. LEFT join and GROUP BY the key of the left table';
EXPLAIN SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k);
SELECT count(), sum(c), max(c) FROM
    (SELECT e.key AS k, count() AS c FROM t_coloc_chain_events AS e LEFT JOIN t_coloc_chain_groups AS g ON e.key = g.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_chain_events;
DROP TABLE t_coloc_chain_groups;
DROP TABLE t_coloc_chain_details;
