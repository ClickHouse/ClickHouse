-- A `PARTITION BY` component is not used for a co-located join when its value depends on the time zone of the
-- session that writes the rows. The two tables of each pair in this test have equal partition keys, but their
-- rows are written in different time zones, so equal join keys are in different partitions. A co-located join
-- of such a pair would lose rows, so the join must have an exchange and must return the same result as on one node.

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

DROP TABLE IF EXISTS t_layout_zone_left;
DROP TABLE IF EXISTS t_layout_zone_right;
DROP TABLE IF EXISTS t_layout_folded_left;
DROP TABLE IF EXISTS t_layout_folded_right;

-- The cost of a read by partition value counts marks, so the tables pin their granularity.
-- `toDate` of a number uses the time zone of the session that writes the rows.
CREATE TABLE t_layout_zone_left (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY toDate(k) ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_layout_zone_right (k UInt64, w UInt64)
ENGINE = MergeTree PARTITION BY toDate(k) ORDER BY w SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A constant that a function computes: an `INSERT` computes it again with the time zone of its session.
CREATE TABLE t_layout_folded_left (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(k, toDate(1767225600)) % 8 ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_layout_folded_right (k UInt64, w UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(k, toDate(1767225600)) % 8 ORDER BY w SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_layout_zone_left;
SYSTEM STOP MERGES t_layout_zone_right;
SYSTEM STOP MERGES t_layout_folded_left;
SYSTEM STOP MERGES t_layout_folded_right;

-- 2000 keys in each right table, each key two times in the left table: every join returns 4000 rows.
-- Timestamps in the first minutes of 8 days in UTC. In `Pacific/Honolulu` each of them is in the day before.
INSERT INTO t_layout_zone_left SELECT 1767225600 + (number % 8) * 86400 + intDiv(number % 2000, 8), number FROM numbers(4000)
SETTINGS session_timezone = 'UTC';
INSERT INTO t_layout_zone_right SELECT 1767225600 + (number % 8) * 86400 + intDiv(number, 8), number % 7 FROM numbers(2000)
SETTINGS session_timezone = 'Pacific/Honolulu';
INSERT INTO t_layout_folded_left SELECT number % 2000, number FROM numbers(4000)
SETTINGS session_timezone = 'UTC';
INSERT INTO t_layout_folded_right SELECT number, number % 7 FROM numbers(2000)
SETTINGS session_timezone = 'Pacific/Honolulu';

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_layout_zone_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_zone_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_folded_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}, "t_layout_folded_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100000000}}}';

SELECT '-- 1. toDate of a number, rows written in two time zones: not co-located';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_layout_zone_left AS l INNER JOIN t_layout_zone_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_zone_left AS l INNER JOIN t_layout_zone_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_zone_left AS l INNER JOIN t_layout_zone_right AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

SELECT '-- 2. a constant computed by a function, rows written in two time zones: not co-located';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_layout_folded_left AS l INNER JOIN t_layout_folded_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_folded_left AS l INNER JOIN t_layout_folded_right AS r ON l.k = r.k;
SELECT count(), sum(l.v), sum(r.w) FROM t_layout_folded_left AS l INNER JOIN t_layout_folded_right AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

DROP TABLE t_layout_zone_left;
DROP TABLE t_layout_zone_right;
DROP TABLE t_layout_folded_left;
DROP TABLE t_layout_folded_right;
