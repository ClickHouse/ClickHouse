-- Joins that `distributed_plan_use_primary_key_range_layout` must not divide by key ranges: the two reads could
-- put rows with equal join keys into different ranges, or the read cannot keep the rows of one range. Each such join
-- gets a plan with an exchange. The first case is the control: the same table with itself is divided by ranges.
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

DROP TABLE IF EXISTS t_range_unsup_base;
DROP TABLE IF EXISTS t_range_unsup_u32;
DROP TABLE IF EXISTS t_range_unsup_desc;
DROP TABLE IF EXISTS t_range_unsup_nullable;
DROP TABLE IF EXISTS t_range_unsup_expr;
DROP TABLE IF EXISTS t_range_unsup_replacing;

CREATE TABLE t_range_unsup_base (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_unsup_u32 (key UInt32, value UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_unsup_desc (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY (key DESC) SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi', allow_experimental_reverse_key = 1;
CREATE TABLE t_range_unsup_nullable (key Nullable(UInt64), value UInt64)
ENGINE = MergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
CREATE TABLE t_range_unsup_expr (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY (intDiv(key, 10), key) SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';
CREATE TABLE t_range_unsup_replacing (key UInt64, value UInt64)
ENGINE = ReplacingMergeTree ORDER BY key SETTINGS auto_statistics_types = '', index_granularity = 128, index_granularity_bytes = '10Mi';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_range_unsup_base;
SYSTEM STOP MERGES t_range_unsup_u32;
SYSTEM STOP MERGES t_range_unsup_desc;
SYSTEM STOP MERGES t_range_unsup_nullable;
SYSTEM STOP MERGES t_range_unsup_expr;
SYSTEM STOP MERGES t_range_unsup_replacing;

INSERT INTO t_range_unsup_base SELECT number % 3000, number FROM numbers(12000);
INSERT INTO t_range_unsup_base SELECT number % 3000 + 1500, number FROM numbers(9000);
INSERT INTO t_range_unsup_u32 SELECT number, number FROM numbers(4000);
INSERT INTO t_range_unsup_desc SELECT number, number FROM numbers(4000);
INSERT INTO t_range_unsup_nullable SELECT number, number FROM numbers(4000);
INSERT INTO t_range_unsup_expr SELECT number, number FROM numbers(4000);
INSERT INTO t_range_unsup_replacing SELECT number, number FROM numbers(4000);
INSERT INTO t_range_unsup_replacing SELECT number, number + 1 FROM numbers(4000);

SET param__internal_join_table_stat_hints = '{"t_range_unsup_base": {"cardinality": 1000000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_unsup_u32": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_unsup_desc": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_unsup_nullable": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_unsup_expr": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}, "t_range_unsup_replacing": {"cardinality": 100000000, "avg_row_bytes": 32, "distinct_keys": {"key": 100000000}}}';
SET distributed_plan_use_primary_key_range_layout = 1;

SELECT '-- 1. control: equal keys';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 2. the key types are not equal';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_u32 AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_u32 AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_u32 AS b ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 3. one key is descending';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_desc AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_desc AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_desc AS b ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 4. Nullable keys';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_nullable AS a INNER JOIN t_range_unsup_nullable AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_nullable AS a INNER JOIN t_range_unsup_nullable AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_nullable AS a INNER JOIN t_range_unsup_nullable AS b ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 5. the first key column is an expression';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_expr AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_expr AS b ON a.key = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_expr AS b ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 6. FINAL';
EXPLAIN SELECT count(), sum(b.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_replacing AS b FINAL ON a.key = b.key;
SELECT count(), sum(b.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_replacing AS b FINAL ON a.key = b.key;
SELECT count(), sum(b.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_replacing AS b FINAL ON a.key = b.key
SETTINGS make_distributed_plan = 0;

SELECT '-- 7. the join compares an expression of the key';
EXPLAIN SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key + 1 = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key + 1 = b.key;
SELECT count(), sum(a.value) FROM t_range_unsup_base AS a INNER JOIN t_range_unsup_base AS b ON a.key + 1 = b.key
SETTINGS make_distributed_plan = 0;

DROP TABLE t_range_unsup_base;
DROP TABLE t_range_unsup_u32;
DROP TABLE t_range_unsup_desc;
DROP TABLE t_range_unsup_nullable;
DROP TABLE t_range_unsup_expr;
DROP TABLE t_range_unsup_replacing;
