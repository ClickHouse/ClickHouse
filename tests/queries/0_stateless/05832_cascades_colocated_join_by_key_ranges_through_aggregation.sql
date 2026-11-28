-- The key ranges of `distributed_plan_use_primary_key_range_layout` through an aggregation and through a `DISTINCT`.
-- A final aggregation by the key columns, or a `DISTINCT` on them, keeps each result row on the node that read its
-- key range. So a join above it can use the ranges of the tables below: the join selects the borders for all the
-- reads, and the aggregation passes them on to its input. The plans have no exchange below the join.
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
-- The test profile also limits the size of a `DISTINCT`, which keeps each final `DISTINCT` on one node.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
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

DROP TABLE IF EXISTS t_range_through_events;
DROP TABLE IF EXISTS t_range_through_groups;

-- The ranges come from the marks of the primary index, so the tables pin their granularity.
CREATE TABLE t_range_through_events (key UInt64, seq UInt32, value UInt64)
ENGINE = MergeTree ORDER BY (key, seq) SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_through_groups (key UInt64, weight UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 64, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_range_through_events;
SYSTEM STOP MERGES t_range_through_groups;

INSERT INTO t_range_through_events SELECT number % 4000, number % 7, number FROM numbers(20000);
INSERT INTO t_range_through_events SELECT number % 4000 + 2000, number % 5, number FROM numbers(12000);
INSERT INTO t_range_through_groups SELECT number, number % 11 FROM numbers(5000);
INSERT INTO t_range_through_groups SELECT number + 2500, number % 13 FROM numbers(5000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_range_through_events": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000, "seq": 100000000}}, "t_range_through_groups": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}}';
SET distributed_plan_use_primary_key_range_layout = 1;

SELECT '-- 1. join of a table with a DISTINCT of the key of another table';
EXPLAIN SELECT count(), sum(e.value) FROM t_range_through_events AS e INNER JOIN (SELECT DISTINCT key FROM t_range_through_groups) AS d ON e.key = d.key;
SELECT count(), sum(e.value) FROM t_range_through_events AS e INNER JOIN (SELECT DISTINCT key FROM t_range_through_groups) AS d ON e.key = d.key;
SELECT count(), sum(e.value) FROM t_range_through_events AS e INNER JOIN (SELECT DISTINCT key FROM t_range_through_groups) AS d ON e.key = d.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. an aggregation by the first key column above an aggregation by two key columns';
EXPLAIN SELECT count(), sum(m), sum(n) FROM
    (SELECT key, max(c) AS m, count() AS n FROM (SELECT key, seq, count() AS c FROM t_range_through_events GROUP BY key, seq) GROUP BY key);
SELECT count(), sum(m), sum(n) FROM
    (SELECT key, max(c) AS m, count() AS n FROM (SELECT key, seq, count() AS c FROM t_range_through_events GROUP BY key, seq) GROUP BY key);
SELECT count(), sum(m), sum(n) FROM
    (SELECT key, max(c) AS m, count() AS n FROM (SELECT key, seq, count() AS c FROM t_range_through_events GROUP BY key, seq) GROUP BY key)
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. the join compares a result of an aggregate function: the aggregation does not keep ranges of it';
EXPLAIN SELECT count(), sum(g.weight) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x INNER JOIN t_range_through_groups AS g ON x.c = g.key;
SELECT count(), sum(g.weight) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x INNER JOIN t_range_through_groups AS g ON x.c = g.key;
SELECT count(), sum(g.weight) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x INNER JOIN t_range_through_groups AS g ON x.c = g.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. the aggregation is not by the first key column';
EXPLAIN SELECT count(), sum(x.c), sum(g.weight) FROM
    (SELECT seq, count() AS c FROM t_range_through_events GROUP BY seq) AS x INNER JOIN t_range_through_groups AS g ON x.seq = g.key;
SELECT count(), sum(x.c), sum(g.weight) FROM
    (SELECT seq, count() AS c FROM t_range_through_events GROUP BY seq) AS x INNER JOIN t_range_through_groups AS g ON x.seq = g.key;
SELECT count(), sum(x.c), sum(g.weight) FROM
    (SELECT seq, count() AS c FROM t_range_through_events GROUP BY seq) AS x INNER JOIN t_range_through_groups AS g ON x.seq = g.key
SETTINGS make_distributed_plan = 0;

-- One set of borders for the two tables is less equal for each table than its own borders, and with the costs of this
-- small test the plan with two shuffles of the aggregated rows is close to it. The network weight makes the choice clear.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":1000,"sequential_weight":32}';
SELECT '-- 5. join of two aggregations by the key';
EXPLAIN SELECT count(), sum(x.c), sum(y.s) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x
    INNER JOIN (SELECT key, sum(weight) AS s FROM t_range_through_groups GROUP BY key) AS y ON x.key = y.key;
SELECT count(), sum(x.c), sum(y.s) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x
    INNER JOIN (SELECT key, sum(weight) AS s FROM t_range_through_groups GROUP BY key) AS y ON x.key = y.key;
SELECT count(), sum(x.c), sum(y.s) FROM
    (SELECT key, count() AS c FROM t_range_through_events GROUP BY key) AS x
    INNER JOIN (SELECT key, sum(weight) AS s FROM t_range_through_groups GROUP BY key) AS y ON x.key = y.key
SETTINGS make_distributed_plan = 0;

DROP TABLE t_range_through_events;
DROP TABLE t_range_through_groups;
