-- Cascades runs a `DISTINCT` on several nodes only where the result stays the same, and prices it on the rows it
-- gets. The cost config makes an exchange cheap next to a read, as in 05406, so that the distributed plans are the
-- cheapest on a small table.
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
SET use_statistics = 1;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_ddl;
-- The number of distinct values decides the plans, so the column statistics that count them are pinned.
CREATE TABLE t_ddl (k UInt64, w UInt8) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
SET materialize_statistics_on_insert = 1;
INSERT INTO t_ddl SELECT intDiv(number, 4), number % 5 FROM numbers(40000);

SELECT '-- the self-join on the DISTINCT column runs the final DISTINCT on every node';
EXPLAIN SELECT DISTINCT s1.k FROM t_ddl AS s1 JOIN t_ddl AS s2 ON s1.k = s2.k WHERE s1.w <> s2.w;
SELECT '-- with max_rows_in_distinct the final DISTINCT stays on one node, which checks the limit on all 10000 values';
EXPLAIN SELECT DISTINCT s1.k FROM t_ddl AS s1 JOIN t_ddl AS s2 ON s1.k = s2.k WHERE s1.w <> s2.w
SETTINGS max_rows_in_distinct = 6000;
SELECT count() FROM (SELECT DISTINCT s1.k FROM t_ddl AS s1 JOIN t_ddl AS s2 ON s1.k = s2.k WHERE s1.w <> s2.w)
SETTINGS max_rows_in_distinct = 6000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- The costs of the reads depend on the sizes of the parts, so the cases below check the cost of the final DISTINCT
-- step alone: its cost minus the cost of its input.
SELECT '-- a key without an NDV: the final DISTINCT keeps the 4000 rows the preliminary one takes for its cost, a tenth of the input';
-- It inserts the 4000 rows and stores them, at least 8 bytes each: at least 4000 + 32 * 4000 + 4000 * 8. A second
-- default reduction would leave it 400 rows to store.
SELECT costs[1] - costs[2] >= 4000 + 32 * 4000 + 4000 * 8 FROM (
    SELECT groupArray(toFloat64(extract(explain, 'cost: ([0-9.]+)'))) AS costs
    FROM (EXPLAIN estimates = 1 SELECT DISTINCT k + w FROM t_ddl) WHERE explain LIKE '%Distinct (%');

SELECT '-- the 5 values of w are on all 8 nodes: the gather moves 40 rows, and the final DISTINCT inserts all of them';
EXPLAIN SELECT DISTINCT w FROM t_ddl;
-- At least 40 + 32 * 40; priced on the 5 distinct values, it would be about 170.
SELECT costs[1] - costs[2] >= 40 + 32 * 40 FROM (
    SELECT groupArray(toFloat64(extract(explain, 'cost: ([0-9.]+)'))) AS costs
    FROM (EXPLAIN estimates = 1 SELECT DISTINCT w FROM t_ddl) WHERE trimLeft(explain) LIKE 'Distinct (Local%' OR trimLeft(explain) LIKE 'GatherExchange%');

DROP TABLE t_ddl;
