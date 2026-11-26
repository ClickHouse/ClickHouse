-- Cascades runs a `DISTINCT` on several nodes. The preliminary `DISTINCT` deduplicates on every node wherever its
-- input is, so a gather above it moves only the distinct rows. The final `DISTINCT` runs on every node when its
-- input is split by the `DISTINCT` columns, e.g. by the key of a shuffle join, and only its result is gathered.
-- The cost config makes an exchange cheap next to a read, so that these plans are the cheapest on a small table,
-- and still dear enough that a shuffle is not added only to deduplicate a few values on every node.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET param__internal_cascades_cost_config = '{"sequential_weight":32,"network_weight":1,"exchange_fixed_overhead":1000,"work_weight":1}';
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET explain_query_plan_default = 'legacy';
-- A `DISTINCT` with a size limit stays on one node, and the test config sets one.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

DROP TABLE IF EXISTS t_dd_sales;
-- The number of distinct values decides the plans, so the column statistics that count them are pinned.
CREATE TABLE t_dd_sales (order_number UInt64, warehouse UInt8) ENGINE = MergeTree ORDER BY order_number
SETTINGS auto_statistics_types = 'basic, uniq_v2';
SET materialize_statistics_on_insert = 1;
INSERT INTO t_dd_sales SELECT intDiv(number, 4), number % 5 FROM numbers(40000);

SELECT '-- a self-join on the DISTINCT column: both DISTINCT steps run on every node, below the gather';
EXPLAIN SELECT DISTINCT s1.order_number FROM t_dd_sales AS s1 JOIN t_dd_sales AS s2 ON s1.order_number = s2.order_number
WHERE s1.warehouse <> s2.warehouse
SETTINGS enable_join_runtime_filters = 0, use_statistics = 1;
SELECT count(), sum(order_number) FROM (
    SELECT DISTINCT s1.order_number FROM t_dd_sales AS s1 JOIN t_dd_sales AS s2 ON s1.order_number = s2.order_number
    WHERE s1.warehouse <> s2.warehouse);
SELECT count(), sum(order_number) FROM (
    SELECT DISTINCT s1.order_number FROM t_dd_sales AS s1 JOIN t_dd_sales AS s2 ON s1.order_number = s2.order_number
    WHERE s1.warehouse <> s2.warehouse)
SETTINGS make_distributed_plan = 0;

SELECT '-- a DISTINCT of a read: the preliminary DISTINCT runs on every node, the final one after the gather';
EXPLAIN SELECT DISTINCT warehouse FROM t_dd_sales SETTINGS use_statistics = 1;
SELECT DISTINCT warehouse FROM t_dd_sales ORDER BY warehouse;

DROP TABLE t_dd_sales;
