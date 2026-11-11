-- When the join order optimizer gives Cascades several join orders to choose from, the joins of each order are
-- hidden inside one step until Cascades picks an order. So join runtime filters are placed after Cascades, on the
-- chosen plan, and are pushed down below the exchanges into `PREWHERE` of the reads, together with the conditions
-- that are already there. The plan checks pin the settings that change the chosen order or the plan text.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":32,"network_weight":1,"exchange_fixed_overhead":1,"work_weight":1}';
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET distributed_plan_join_runtime_filters = 1;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET cascades_join_order_alternatives = 4;
SET query_plan_optimize_join_order_limit = 10;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_rfa_fact;
DROP TABLE IF EXISTS t_rfa_mid;
DROP TABLE IF EXISTS t_rfa_dim;
DROP TABLE IF EXISTS t_rfa_dim2;
CREATE TABLE t_rfa_fact (k UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_rfa_mid (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_rfa_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_rfa_dim2 (k UInt64, b UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
-- The chosen order depends on the column statistics.
SET materialize_statistics_on_insert = 1;
INSERT INTO t_rfa_fact SELECT number % 10000, number % 500, number FROM numbers(200000);
INSERT INTO t_rfa_mid SELECT number, number FROM numbers(10000);
INSERT INTO t_rfa_dim SELECT number, if(number < 10, 'keep', 'skip') FROM numbers(10000);
INSERT INTO t_rfa_dim2 SELECT number, number % 500, if(number < 10, 'keep', 'skip') FROM numbers(10000);

SELECT '-- dpsub: the filter is added to the existing PREWHERE of the fact read';
SELECT arrayFirst((line, index) -> index > arrayFirstIndex(x -> x LIKE 'ReadFromMergeTree (%.t_rfa_fact)', lines) AND line LIKE 'Prewhere filter column:%', lines, arrayEnumerate(lines)) LIKE '%greater(__table1.v, 10_UInt8)%__applyFilter(%'
FROM (
    SELECT groupArray(trimLeft(explain)) AS lines FROM (
        EXPLAIN actions = 1
        SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k JOIN t_rfa_dim AS d ON f.k = d.k
        WHERE d.name = 'keep' AND f.v > 10
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
            optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0,
            query_plan_join_swap_table = 'false', query_plan_convert_outer_join_to_inner_join = 0, use_statistics = 1
    )
);
SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k JOIN t_rfa_dim AS d ON f.k = d.k
WHERE d.name = 'keep' AND f.v > 10
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT '-- dphyp: the same';
SELECT arrayFirst((line, index) -> index > arrayFirstIndex(x -> x LIKE 'ReadFromMergeTree (%.t_rfa_fact)', lines) AND line LIKE 'Prewhere filter column:%', lines, arrayEnumerate(lines)) LIKE '%greater(__table1.v, 10_UInt8)%__applyFilter(%'
FROM (
    SELECT groupArray(trimLeft(explain)) AS lines FROM (
        EXPLAIN actions = 1
        SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k JOIN t_rfa_dim AS d ON f.k = d.k
        WHERE d.name = 'keep' AND f.v > 10
        SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
            optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0,
            query_plan_join_swap_table = 'false', query_plan_convert_outer_join_to_inner_join = 0, use_statistics = 1
    )
);
SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k JOIN t_rfa_dim AS d ON f.k = d.k
WHERE d.name = 'keep' AND f.v > 10
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT '-- reference';
SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k JOIN t_rfa_dim AS d ON f.k = d.k
WHERE d.name = 'keep' AND f.v > 10
SETTINGS make_distributed_plan = 0;

SELECT '-- the filter of the outer join is pushed through the inner join into the reads of both its inputs';
SELECT hasAny(extractAll(arrayFirst((line, index) -> index > arrayFirstIndex(x -> x LIKE 'ReadFromMergeTree (%.t_rfa_fact)', lines) AND line LIKE 'Prewhere filter column:%', lines, arrayEnumerate(lines)), '_runtime_filter_\\d+'), extractAll(arrayFirst((line, index) -> index > arrayFirstIndex(x -> x LIKE 'ReadFromMergeTree (%.t_rfa_mid)', lines) AND line LIKE 'Prewhere filter column:%', lines, arrayEnumerate(lines)), '_runtime_filter_\\d+'))
FROM (
    SELECT groupArray(trimLeft(explain)) AS lines FROM (
        EXPLAIN actions = 1
        SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k RIGHT JOIN t_rfa_dim AS d ON f.k = d.k
        WHERE d.name = 'keep'
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
            optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0,
            query_plan_join_swap_table = 'false', query_plan_convert_outer_join_to_inner_join = 0, use_statistics = 1
    )
);
SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k RIGHT JOIN t_rfa_dim AS d ON f.k = d.k
WHERE d.name = 'keep'
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_join_swap_table = 'false';
SELECT sum(f.v), sum(m.x) FROM t_rfa_fact AS f JOIN t_rfa_mid AS m ON f.k = m.k RIGHT JOIN t_rfa_dim AS d ON f.k = d.k
WHERE d.name = 'keep'
SETTINGS make_distributed_plan = 0;

-- The plan shows the two filters of one join under the same name. Without a check, the second pass that moves conditions
-- to `PREWHERE` could join them by name into one filter.
SELECT '-- two filters of one join in the same PREWHERE, with the second PREWHERE pass';
SELECT length(extractAll(arrayFirst((line, index) -> index > arrayFirstIndex(x -> x LIKE 'ReadFromMergeTree (%.t_rfa_fact)', lines) AND line LIKE 'Prewhere filter column:%', lines, arrayEnumerate(lines)), '__applyFilter\\('))
FROM (
    SELECT groupArray(trimLeft(explain)) AS lines FROM (
        EXPLAIN actions = 1
        SELECT sum(f.v) FROM t_rfa_fact AS f JOIN t_rfa_dim2 AS d ON f.k = d.k AND f.b = d.b
        WHERE d.name = 'keep' AND f.v > 10
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', optimize_prewhere_after_pushdown = 1,
            optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0,
            query_plan_join_swap_table = 'false', query_plan_convert_outer_join_to_inner_join = 0, use_statistics = 1
    )
);
SELECT sum(f.v) FROM t_rfa_fact AS f JOIN t_rfa_dim2 AS d ON f.k = d.k AND f.b = d.b
WHERE d.name = 'keep' AND f.v > 10
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', optimize_prewhere_after_pushdown = 1;
SELECT sum(f.v) FROM t_rfa_fact AS f JOIN t_rfa_dim2 AS d ON f.k = d.k AND f.b = d.b
WHERE d.name = 'keep' AND f.v > 10
SETTINGS make_distributed_plan = 0;

DROP TABLE t_rfa_fact;
DROP TABLE t_rfa_mid;
DROP TABLE t_rfa_dim;
DROP TABLE t_rfa_dim2;
