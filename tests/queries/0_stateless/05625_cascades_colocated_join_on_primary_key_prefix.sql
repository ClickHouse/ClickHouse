-- A co-located join puts a hash join directly above two reads of a distributed plan. When the join key is a prefix of the
-- primary key and the hash join cannot spill, a join on one node is split by primary-key ranges. That split must not apply
-- to the reads of a task: it builds the reads again from all parts of the table, and every task would then read all rows.

SET explain_query_plan_default = 'legacy';
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
-- The conditions of the join split by primary-key ranges: a hash join with no runtime filters that cannot spill.
SET query_plan_join_shard_by_pk_ranges = 1;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'hash';
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
-- The test pins a full EXPLAIN output, so the randomized settings that shape the plan are pinned.
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

DROP TABLE IF EXISTS t_coloc_pk_left;
DROP TABLE IF EXISTS t_coloc_pk_right;

-- The sorting key starts with the join key.
CREATE TABLE t_coloc_pk_left (key UInt64, value UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(key) % 8 ORDER BY key SETTINGS auto_statistics_types = '';
CREATE TABLE t_coloc_pk_right (key UInt64, weight UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(key) % 8 ORDER BY key SETTINGS auto_statistics_types = '';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_pk_left;
SYSTEM STOP MERGES t_coloc_pk_right;

INSERT INTO t_coloc_pk_left SELECT number % 2000, number FROM numbers(20000);
INSERT INTO t_coloc_pk_right SELECT number + 1000, number FROM numbers(2000);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_coloc_pk_left": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"key": 100000000}}, "t_coloc_pk_right": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"key": 100000000}}}';
SET distributed_plan_use_partition_key_layout = 1;

SELECT '-- the join is co-located';
EXPLAIN SELECT count(), sum(l.value), sum(r.weight) FROM t_coloc_pk_left AS l INNER JOIN t_coloc_pk_right AS r ON l.key = r.key;

SELECT '-- every row is read one time: the result is the result of the plain engine';
SELECT count(), sum(l.value), sum(r.weight) FROM t_coloc_pk_left AS l INNER JOIN t_coloc_pk_right AS r ON l.key = r.key;
SELECT count(), sum(l.value), sum(r.weight) FROM t_coloc_pk_left AS l INNER JOIN t_coloc_pk_right AS r ON l.key = r.key
SETTINGS make_distributed_plan = 0;

SELECT '-- the same for a join with GROUP BY the key in one fragment';
SELECT count(), sum(c), sum(s) FROM
    (SELECT l.key AS k, count() AS c, sum(l.value) AS s FROM t_coloc_pk_left AS l INNER JOIN t_coloc_pk_right AS r ON l.key = r.key GROUP BY k);
SELECT count(), sum(c), sum(s) FROM
    (SELECT l.key AS k, count() AS c, sum(l.value) AS s FROM t_coloc_pk_left AS l INNER JOIN t_coloc_pk_right AS r ON l.key = r.key GROUP BY k)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_pk_left;
DROP TABLE t_coloc_pk_right;
