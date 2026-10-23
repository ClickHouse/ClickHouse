-- The value of `optimize_constant_columns_after_filter` is taken from the context of the query the filter belongs to,
-- so the `SETTINGS` clause of a subquery overrides the one of the outer query.

-- The checks below depend on the plan shape, which parallel replicas change.
SET enable_parallel_replicas = 0;

SELECT 'inner disabled, outer enabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM values('x UInt64', 1, 2, 1)
    WHERE x = 1
    SETTINGS optimize_constant_columns_after_filter = 0
)
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'inner enabled, outer disabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM values('x UInt64', 1, 2, 1)
    WHERE x = 1
    SETTINGS optimize_constant_columns_after_filter = 1
)
SETTINGS optimize_constant_columns_after_filter = 0;

-- Only a part of the predicate moves to `PREWHERE`, the rest stays in a filter above the read.
-- The sparse serialization of `a` is disabled: otherwise `a = 1` may look more expensive than `x = ...`
-- and `x = ...` is moved to `PREWHERE` instead.
DROP TABLE IF EXISTS t_filter_const_subquery_settings;
CREATE TABLE t_filter_const_subquery_settings (a UInt8, x String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_filter_const_subquery_settings SELECT number % 2, repeat('x', 1000) FROM numbers(4);

SELECT 'partial PREWHERE, inner disabled, outer enabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM t_filter_const_subquery_settings WHERE a = 1 AND x = repeat('x', 1000)
    SETTINGS optimize_constant_columns_after_filter = 0
)
SETTINGS optimize_constant_columns_after_filter = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    move_all_conditions_to_prewhere = 0, query_plan_merge_filters = 0, query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

SELECT 'partial PREWHERE, inner enabled, outer disabled';
SELECT dumpColumnStructure(x) LIKE '%Const%'
FROM
(
    SELECT x FROM t_filter_const_subquery_settings WHERE a = 1 AND x = repeat('x', 1000)
    SETTINGS optimize_constant_columns_after_filter = 1
)
SETTINGS optimize_constant_columns_after_filter = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    move_all_conditions_to_prewhere = 0, query_plan_merge_filters = 0, query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

-- The plan is serialized and executed on the remote side, where the column structure is checked.
SELECT 'serialized plan, inner disabled, outer enabled';
SELECT s
FROM
(
    SELECT dumpColumnStructure(x) LIKE '%Const%' AS s
    FROM remote('127.0.0.2', currentDatabase(), t_filter_const_subquery_settings)
    WHERE x = repeat('x', 1000) AND a = 1
    SETTINGS optimize_constant_columns_after_filter = 0
)
SETTINGS optimize_constant_columns_after_filter = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, optimize_move_to_prewhere = 0,
    query_plan_merge_filters = 1;

SELECT 'serialized plan, inner enabled, outer disabled';
SELECT s
FROM
(
    SELECT dumpColumnStructure(x) LIKE '%Const%' AS s
    FROM remote('127.0.0.2', currentDatabase(), t_filter_const_subquery_settings)
    WHERE x = repeat('x', 1000) AND a = 1
    SETTINGS optimize_constant_columns_after_filter = 1
)
SETTINGS optimize_constant_columns_after_filter = 0, serialize_query_plan = 1, prefer_localhost_replica = 0, optimize_move_to_prewhere = 0,
    query_plan_merge_filters = 1;

DROP TABLE t_filter_const_subquery_settings;

-- Lazy materialization splits the filter and rebuilds it below `JoinLazyColumnsStep`.
DROP TABLE IF EXISTS t_filter_const_subquery_settings_lazy;
CREATE TABLE t_filter_const_subquery_settings_lazy (a UInt64, x String, p String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filter_const_subquery_settings_lazy SELECT number, repeat('x', 1000), repeat('p', 1000) FROM numbers(10000);

SELECT 'lazy materialization, inner disabled, outer enabled';
SELECT s, length(p)
FROM
(
    SELECT dumpColumnStructure(x) LIKE '%Const%' AS s, p
    FROM t_filter_const_subquery_settings_lazy
    WHERE x = repeat('x', 1000)
    ORDER BY s, a DESC LIMIT 2
    SETTINGS optimize_constant_columns_after_filter = 0
)
SETTINGS optimize_constant_columns_after_filter = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
    query_plan_optimize_lazy_materialization = 1;

SELECT 'lazy materialization, inner enabled, outer disabled';
SELECT s, length(p)
FROM
(
    SELECT dumpColumnStructure(x) LIKE '%Const%' AS s, p
    FROM t_filter_const_subquery_settings_lazy
    WHERE x = repeat('x', 1000)
    ORDER BY s, a DESC LIMIT 2
    SETTINGS optimize_constant_columns_after_filter = 1
)
SETTINGS optimize_constant_columns_after_filter = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
    query_plan_optimize_lazy_materialization = 1;

DROP TABLE t_filter_const_subquery_settings_lazy;
