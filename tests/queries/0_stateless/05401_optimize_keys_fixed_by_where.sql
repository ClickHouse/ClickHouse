-- { echo }

SET enable_analyzer = 1;
SET optimize_keys_fixed_by_where = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (tenant String, day Date, v UInt64) ENGINE = MergeTree ORDER BY (tenant, day);
INSERT INTO t VALUES ('x', '2024-01-01', 1), ('x', '2024-01-01', 2), ('x', '2024-01-02', 3), ('y', '2024-01-01', 4);

-- GROUP BY and ORDER BY: the key fixed by WHERE becomes a constant, which the planner drops from the aggregation keys.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT tenant, day, sum(v) FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY tenant, day;
SELECT tenant, day, sum(v) FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY tenant, day;

-- At least one ORDER BY element is kept, so the rows are still read in the order of the table.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT day FROM t WHERE tenant = 'x' ORDER BY tenant;

-- An expression of the key after the aggregation is replaced too.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT concat(tenant, '-', toString(day)) AS s, count() FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY s;
SELECT concat(tenant, '-', toString(day)) AS s, count() FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY s;
-- The key is evaluated for each row like the column, so a function of it does not throw when no row is left.
SELECT intDiv(1, number), count() FROM numbers(3) WHERE number = 0 GROUP BY number HAVING count() > 1;
-- Every clause after the aggregation is rewritten.
SELECT number, count() FROM numbers(3) WHERE number = 1 GROUP BY number LIMIT AFTER number = 1;

-- PARTITION BY of a window function.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT day, v, row_number() OVER (PARTITION BY tenant, day ORDER BY v) FROM t WHERE tenant = 'x' ORDER BY day, v;
SELECT day, v, row_number() OVER (PARTITION BY tenant, day ORDER BY v) FROM t WHERE tenant = 'x' ORDER BY day, v;

-- ORDER BY is still truncated after the remaining GROUP BY keys.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT tenant, day, sum(v) FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY tenant, day, sum(v) SETTINGS optimize_truncate_order_by_after_group_by_keys = 1;

-- Empty input gives no rows when every key is removed.
SELECT tenant, count() FROM t WHERE tenant = 'z' GROUP BY tenant;
-- Aggregation by constant keys would return a row here, so the key is kept.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT tenant, count() FROM t WHERE tenant = 'z' GROUP BY tenant SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT tenant, count() FROM t WHERE tenant = 'z' GROUP BY tenant SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
-- Also when the other keys are constants.
SELECT tenant, count() FROM t WHERE tenant = 'z' GROUP BY tenant, 'c' SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;

-- The constant takes the type of the key.
DROP TABLE IF EXISTS types;
CREATE TABLE types (lc LowCardinality(Nullable(String)), dt DateTime('UTC')) ENGINE = Memory;
INSERT INTO types VALUES ('a', '2024-01-01 00:00:00'), ('b', '2024-01-02 00:00:00');
SELECT lc, dt, toTypeName(lc), toTypeName(dt), count() FROM types WHERE lc = 'a' AND dt = '2024-01-01 00:00:00' GROUP BY lc, dt;
-- A string is parsed with the format settings of the comparison.
SELECT dt, count() FROM types WHERE dt = '20240101' GROUP BY dt SETTINGS date_time_input_format = 'best_effort';

-- Different floating-point values compare equal (`-0.0 = 0.0`), so the key is kept.
DROP TABLE IF EXISTS floats;
CREATE TABLE floats (x Float64, v UInt64) ENGINE = Memory;
INSERT INTO floats VALUES (0, 1), (-0., 2);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT x, sum(v) FROM floats WHERE x = 0 GROUP BY x;

-- In a self-join the columns differ only by the table alias, so the key of the other side is kept.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT b.tenant, count() FROM t AS a JOIN t AS b ON a.day = b.day WHERE a.tenant = 'x' GROUP BY b.tenant;

-- The totals have default values in place of the keys, so the key is kept.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT tenant, count() FROM t WHERE tenant = 'x' GROUP BY tenant WITH TOTALS;

DROP TABLE floats;
DROP TABLE types;
DROP TABLE t;
