-- An aggregation whose `GROUP BY` keys contain the columns of a `PARTITION BY` component runs with no exchange below it
-- when `distributed_plan_use_partition_key_layout` is set: every node reads the partitions of its bucket
-- (`PartitionedRead`) and has all rows of its groups. Other aggregations must still merge the results of the nodes.

SET explain_query_plan_default = 'legacy';
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
-- The test pins full EXPLAIN outputs, so the randomized settings that shape these plans are pinned.
SET optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET optimize_aggregation_in_order = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET cascades_aggregation_pushdown = 1;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_coloc_agg;
DROP TABLE IF EXISTS t_coloc_agg_monthly;
DROP TABLE IF EXISTS t_coloc_agg_replacing;

-- The cost of a read by partition value counts marks, so the tables pin their granularity.
CREATE TABLE t_coloc_agg (key UInt64, tag LowCardinality(String), value UInt64, day Date)
ENGINE = MergeTree PARTITION BY (day, cityHash64(key) % 8) ORDER BY (tag, value) SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_agg_monthly (day Date, value UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY value SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_coloc_agg_replacing (key UInt64, version UInt64, value UInt64)
ENGINE = ReplacingMergeTree(version) PARTITION BY cityHash64(key) % 8 ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_agg;
SYSTEM STOP MERGES t_coloc_agg_monthly;
SYSTEM STOP MERGES t_coloc_agg_replacing;

-- 2000 keys with 10 rows each. The rows of one key are in two `day` partitions, and always in one hash partition.
INSERT INTO t_coloc_agg
SELECT number % 2000, ['a', 'b', 'c', 'd'][number % 4 + 1], number, toDate('2026-01-01') + intDiv(number, 2000) % 2 FROM numbers(20000);
-- 8 months.
INSERT INTO t_coloc_agg_monthly SELECT toDate('2026-01-01') + number % 240, number FROM numbers(24000);
-- Two versions of every key, in two parts.
INSERT INTO t_coloc_agg_replacing SELECT number, 1, number FROM numbers(4000);
INSERT INTO t_coloc_agg_replacing SELECT number, 2, number * 10 FROM numbers(4000);

-- The hints claim big tables with many groups, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_coloc_agg": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000, "tag": 4}}, "t_coloc_agg_monthly": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"day": 100000000}}, "t_coloc_agg_replacing": {"cardinality": 1000000000, "avg_row_bytes": 24, "distinct_keys": {"key": 100000000}}}';

SELECT '-- 1. GROUP BY the key, the setting is off: an exchange below the aggregation';
EXPLAIN SELECT key, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY key ORDER BY s DESC, key LIMIT 3;

SET distributed_plan_use_partition_key_layout = 1;

SELECT '-- 2. GROUP BY the key, the setting is on: no exchange below the aggregation';
EXPLAIN SELECT key, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY key ORDER BY s DESC, key LIMIT 3;
SELECT key, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY key ORDER BY s DESC, key LIMIT 3;
SELECT key, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY key ORDER BY s DESC, key LIMIT 3 SETTINGS make_distributed_plan = 0;

SELECT '-- 3. GROUP BY the key and one more column';
EXPLAIN SELECT count(), sum(c) FROM (SELECT key, tag, count() AS c FROM t_coloc_agg GROUP BY key, tag);
SELECT count(), sum(c) FROM (SELECT key, tag, count() AS c FROM t_coloc_agg GROUP BY key, tag);
SELECT count(), sum(c) FROM (SELECT key, tag, count() AS c FROM t_coloc_agg GROUP BY key, tag) SETTINGS make_distributed_plan = 0;

SELECT '-- 4. GROUP BY another column: the nodes send partial results, one row for each group';
EXPLAIN SELECT tag, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY tag ORDER BY tag;
SELECT tag, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY tag ORDER BY tag;
SELECT tag, count() AS c, sum(value) AS s FROM t_coloc_agg GROUP BY tag ORDER BY tag SETTINGS make_distributed_plan = 0;

SELECT '-- 5. no GROUP BY: one row';
SELECT count(), sum(value), uniqExact(key) FROM t_coloc_agg;
SELECT count(), sum(value), uniqExact(key) FROM t_coloc_agg SETTINGS make_distributed_plan = 0;

SELECT '-- 6. a table partitioned by month, GROUP BY the date';
EXPLAIN SELECT count(), sum(s) FROM (SELECT day, sum(value) AS s FROM t_coloc_agg_monthly GROUP BY day);
SELECT count(), sum(s) FROM (SELECT day, sum(value) AS s FROM t_coloc_agg_monthly GROUP BY day);
SELECT count(), sum(s) FROM (SELECT day, sum(value) AS s FROM t_coloc_agg_monthly GROUP BY day) SETTINGS make_distributed_plan = 0;

SELECT '-- 7. FINAL: the read is not split by partition value';
-- Without this setting the `FINAL` read merges parts across partitions, and the plan stays on one node.
EXPLAIN SELECT count(), sum(s) FROM (SELECT key, sum(value) AS s FROM t_coloc_agg_replacing FINAL GROUP BY key)
SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count(), sum(s) FROM (SELECT key, sum(value) AS s FROM t_coloc_agg_replacing FINAL GROUP BY key)
SETTINGS enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT count(), sum(s) FROM (SELECT key, sum(value) AS s FROM t_coloc_agg_replacing FINAL GROUP BY key) SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_agg;
DROP TABLE t_coloc_agg_monthly;
DROP TABLE t_coloc_agg_replacing;
