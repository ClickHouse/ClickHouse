-- A join of two tables that have the same expression of the join key in `PARTITION BY` runs with no exchange
-- below the join when `distributed_plan_use_partition_key_layout` is set: every node reads the partitions of its
-- bucket from the two tables (`PartitionedRead`) and joins them. The expression is compared by its structure, so
-- the key columns can have different names.

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

DROP TABLE IF EXISTS t_coloc_events;
DROP TABLE IF EXISTS t_coloc_groups;
DROP TABLE IF EXISTS t_coloc_groups_renamed;

-- The cost of a read by partition value counts marks, so the tables pin their granularity.
CREATE TABLE t_coloc_events (key UInt64, seq UInt32, parent_seq UInt32, tag LowCardinality(String), value UInt64, day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY (tag, seq) SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_groups (key UInt64, owner LowCardinality(String), day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY owner SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_groups_renamed (id UInt64, owner LowCardinality(String), day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(id) % 8) ORDER BY owner SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_events;
SYSTEM STOP MERGES t_coloc_groups;
SYSTEM STOP MERGES t_coloc_groups_renamed;

-- 2000 keys with 10 events each. The events of one key are in two `day` partitions, and always in one hash partition.
INSERT INTO t_coloc_events
SELECT number % 2000, intDiv(number, 2000) AS seq, if(seq = 0, 1000000, seq - 1), ['a', 'b', 'c', 'd'][number % 4 + 1], number,
    toDate('2026-01-01') + seq % 2
FROM numbers(20000);
-- Keys 1000 .. 2999: one half of them has events. All groups are in one `day` partition.
INSERT INTO t_coloc_groups SELECT number + 1000, ['x', 'y', 'z'][number % 3 + 1], toDate('2026-01-01') FROM numbers(2000);
INSERT INTO t_coloc_groups_renamed SELECT * FROM t_coloc_groups;

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node, and a broadcast
-- of the right side is expensive. The tables stay small to keep the test fast.
SET param__internal_join_table_stat_hints = '{"t_coloc_events": {"cardinality": 1000000000, "avg_row_bytes": 40, "distinct_keys": {"key": 100000000, "seq": 10, "parent_seq": 10}}, "t_coloc_groups": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"key": 100000000}}, "t_coloc_groups_renamed": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"id": 100000000}}}';

SELECT '-- 1. two tables, the setting is off: no co-located join';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups AS g ON e.key = g.key;

SET distributed_plan_use_partition_key_layout = 1;

SELECT '-- 2. two tables, the setting is on: no exchange below the join';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups AS g ON e.key = g.key SETTINGS make_distributed_plan = 0;

SELECT '-- 3. self-join on the key and one more column';
EXPLAIN SELECT count(), sum(p.value) FROM t_coloc_events AS p INNER JOIN t_coloc_events AS c ON p.key = c.key AND p.seq = c.parent_seq WHERE c.tag = 'a';
SELECT count(), sum(p.value) FROM t_coloc_events AS p INNER JOIN t_coloc_events AS c ON p.key = c.key AND p.seq = c.parent_seq WHERE c.tag = 'a';
SELECT count(), sum(p.value) FROM t_coloc_events AS p INNER JOIN t_coloc_events AS c ON p.key = c.key AND p.seq = c.parent_seq WHERE c.tag = 'a' SETTINGS make_distributed_plan = 0;

SELECT '-- 4. semi-join with a subquery on the right side';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e LEFT SEMI JOIN (SELECT key FROM t_coloc_groups WHERE owner = 'y') AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e LEFT SEMI JOIN (SELECT key FROM t_coloc_groups WHERE owner = 'y') AS g ON e.key = g.key;
SELECT count(), sum(e.value) FROM t_coloc_events AS e LEFT SEMI JOIN (SELECT key FROM t_coloc_groups WHERE owner = 'y') AS g ON e.key = g.key SETTINGS make_distributed_plan = 0;

SELECT '-- 5. the key column has another name in the right table';
EXPLAIN SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_renamed AS g ON e.key = g.id;
SELECT count(), sum(e.value) FROM t_coloc_events AS e INNER JOIN t_coloc_groups_renamed AS g ON e.key = g.id;

SELECT '-- 6. the join is not on the key: not co-located';
EXPLAIN SELECT count() FROM t_coloc_events AS p INNER JOIN t_coloc_groups AS g ON p.value = g.key;
SELECT count() FROM t_coloc_events AS p INNER JOIN t_coloc_groups AS g ON p.value = g.key;
SELECT count() FROM t_coloc_events AS p INNER JOIN t_coloc_groups AS g ON p.value = g.key SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_events;
DROP TABLE t_coloc_groups;
DROP TABLE t_coloc_groups_renamed;
