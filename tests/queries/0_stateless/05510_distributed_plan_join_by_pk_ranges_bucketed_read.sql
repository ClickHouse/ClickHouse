-- A join over a bucketed distributed read must return the same rows as the local plan:
-- each reader task reads only its bucket, also when the join could be sharded by primary-key ranges.

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1;
-- Fail instead of falling back to a local plan.
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
-- With runtime filters the join is not sharded by primary-key ranges.
SET enable_join_runtime_filters = 0;
-- The CI profile sets a non-zero max_rows_to_group_by, which keeps the aggregation local.
SET max_rows_to_group_by = 0;
-- Keep the plan stable under randomized settings.
SET query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10,
    query_plan_join_swap_table = 'auto', query_plan_convert_outer_join_to_inner_join = 1,
    query_plan_convert_any_join_to_semi_or_anti_join = 1, query_plan_merge_filter_into_join_condition = 1;
-- Four reader tasks for t_orders and a broadcast of the small t_lineitem.
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":1}';
SET param__internal_join_table_stat_hints = '{
    "t_orders":   { "cardinality": 1000000, "avg_row_bytes": 16, "distinct_keys": { "o_orderkey": 1000000 } },
    "t_lineitem": { "cardinality": 20,      "avg_row_bytes": 12, "distinct_keys": { "l_orderkey": 20 } }
}';
-- A non-spilling HashJoin, which is the join that can be sharded by primary-key ranges.
SET query_plan_join_shard_by_pk_ranges = 1, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_orders;
DROP TABLE IF EXISTS t_lineitem;

CREATE TABLE t_orders (o_orderkey UInt64, o_priority String) ENGINE = MergeTree ORDER BY o_orderkey
    SETTINGS index_granularity = 8192, auto_statistics_types = '';
CREATE TABLE t_lineitem (l_orderkey UInt64, l_linenumber UInt32) ENGINE = MergeTree ORDER BY l_orderkey
    SETTINGS index_granularity = 8192, auto_statistics_types = '';

SYSTEM STOP MERGES t_orders;
SYSTEM STOP MERGES t_lineitem;
INSERT INTO t_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250);
INSERT INTO t_orders SELECT number, 'P' || toString(number % 5) FROM numbers(250, 250);
INSERT INTO t_orders SELECT number, 'P' || toString(number % 5) FROM numbers(500, 250);
INSERT INTO t_orders SELECT number, 'P' || toString(number % 5) FROM numbers(750, 250);
INSERT INTO t_lineitem SELECT number, 1 FROM numbers(20);

SELECT count() FROM t_orders WHERE EXISTS (SELECT 1 FROM t_lineitem WHERE l_orderkey = o_orderkey);
SELECT count() FROM t_orders WHERE EXISTS (SELECT 1 FROM t_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT count() FROM t_orders WHERE NOT EXISTS (SELECT 1 FROM t_lineitem WHERE l_orderkey = o_orderkey);
SELECT count() FROM t_orders WHERE NOT EXISTS (SELECT 1 FROM t_lineitem WHERE l_orderkey = o_orderkey)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT count() FROM t_orders LEFT SEMI JOIN t_lineitem ON l_orderkey = o_orderkey;
SELECT count() FROM t_orders LEFT SEMI JOIN t_lineitem ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT count() FROM t_orders INNER JOIN t_lineitem ON l_orderkey = o_orderkey;
SELECT count() FROM t_orders INNER JOIN t_lineitem ON l_orderkey = o_orderkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- The plan is distributed: a broadcast join over a bucketed read of t_orders. The outer query over EXPLAIN runs locally.
SELECT count() FROM (EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_orders WHERE EXISTS (SELECT 1 FROM t_lineitem WHERE l_orderkey = o_orderkey))
WHERE explain ILIKE '%Broadcast HashJoin%' OR explain ILIKE '%ParallelRead %.t_orders)%'
SETTINGS distributed_plan_fallback_to_local_execution = 1;

DROP TABLE t_orders;
DROP TABLE t_lineitem;
