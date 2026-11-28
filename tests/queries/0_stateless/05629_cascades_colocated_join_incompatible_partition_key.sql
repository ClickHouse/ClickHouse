-- A join is co-located by a `PARTITION BY` component only when the two tables have equal components for the
-- join key. Another modulus or another type of the key column gives a different component. A floating-point
-- key is never used, because `0.` and `-0.` compare as equal and have different hashes. Each of these joins
-- must have an exchange.

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
SET param__internal_cascades_cluster_node_count = 4;
SET distributed_plan_use_partition_key_layout = 1;

DROP TABLE IF EXISTS t_coloc_events;
DROP TABLE IF EXISTS t_coloc_groups_mod4;
DROP TABLE IF EXISTS t_coloc_groups_u32;
DROP TABLE IF EXISTS t_coloc_float_left;
DROP TABLE IF EXISTS t_coloc_float_right;

-- The cost of a read by partition value counts marks, so the tables pin their granularity.
-- The `day` component is equal in the three tables, but a join on `key` cannot use it.
CREATE TABLE t_coloc_events (key UInt64, value UInt64, day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY value SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_groups_mod4 (key UInt64, owner LowCardinality(String), day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 4) ORDER BY owner SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_groups_u32 (key UInt32, owner LowCardinality(String), day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY owner SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE t_coloc_float_left (f Float64, value UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(f) % 8 ORDER BY value SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_float_right (f Float64, value UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(f) % 8 ORDER BY value SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_events;
SYSTEM STOP MERGES t_coloc_groups_mod4;
SYSTEM STOP MERGES t_coloc_groups_u32;
SYSTEM STOP MERGES t_coloc_float_left;
SYSTEM STOP MERGES t_coloc_float_right;

-- 2000 keys with 10 events each.
INSERT INTO t_coloc_events SELECT number % 2000, number, toDate('2026-01-01') FROM numbers(20000);
-- Keys 1000 .. 2999: one half of them has events.
INSERT INTO t_coloc_groups_mod4 SELECT number + 1000, ['x', 'y', 'z'][number % 3 + 1], toDate('2026-01-01') FROM numbers(2000);
INSERT INTO t_coloc_groups_u32 SELECT number + 1000, ['x', 'y', 'z'][number % 3 + 1], toDate('2026-01-01') FROM numbers(2000);
-- `0.` and `-0.` compare as equal and have different hashes, so they are in different partitions.
INSERT INTO t_coloc_float_left SELECT if(number = 0, 0., number / 4), number FROM numbers(4000);
INSERT INTO t_coloc_float_right SELECT if(number = 0, -0., number / 4), number FROM numbers(2000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_coloc_events": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"key": 100000000}}, "t_coloc_groups_mod4": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"key": 100000000}}, "t_coloc_groups_u32": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"key": 100000000}}, "t_coloc_float_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"f": 100000000}}, "t_coloc_float_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"f": 100000000}}}';

SELECT '-- 1. another modulus in the right table: not co-located';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_mod4 AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_mod4 AS g ON e.key = g.key;

SELECT '-- 2. another key type in the right table: not co-located';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_u32 AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_u32 AS g ON e.key = g.key;

SELECT '-- 3. a floating-point key: not co-located, because equal keys can be in different partitions';
EXPLAIN SELECT count(), sum(l.value) FROM t_coloc_float_left AS l INNER JOIN t_coloc_float_right AS r ON l.f = r.f;
SELECT count(), sum(l.value) FROM t_coloc_float_left AS l INNER JOIN t_coloc_float_right AS r ON l.f = r.f;
SELECT count(), sum(l.value) FROM t_coloc_float_left AS l INNER JOIN t_coloc_float_right AS r ON l.f = r.f SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_events;
DROP TABLE t_coloc_groups_mod4;
DROP TABLE t_coloc_groups_u32;
DROP TABLE t_coloc_float_left;
DROP TABLE t_coloc_float_right;
