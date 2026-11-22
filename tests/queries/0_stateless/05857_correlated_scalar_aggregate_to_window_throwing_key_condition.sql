-- The rewrite of a correlated scalar aggregate into a window copies the conditions on the keys below the window, where
-- they run on every row of the table, also on rows the query never evaluates them on. `equals` of `Decimal(18, 0)` and
-- `UInt64` throws `DECIMAL_OVERFLOW` for a value that does not fit, so it must not be copied: here `keep = 1` filters
-- out that row before `d = n` is evaluated on it.

DROP TABLE IF EXISTS t_correlated_throwing_key_condition;
CREATE TABLE t_correlated_throwing_key_condition (d Decimal(18, 0), n UInt64, keep UInt8, v Int32)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_correlated_throwing_key_condition VALUES (1, 1, 1, 10), (2, 18446744073709551615, 0, 20);

SET enable_analyzer = 1, short_circuit_function_evaluation = 'force_enable', optimize_move_to_prewhere = 0, query_plan_filter_push_down = 0;

SELECT o.d, (SELECT max(v) FROM t_correlated_throwing_key_condition AS s WHERE s.d = o.d AND s.n = o.n)
FROM t_correlated_throwing_key_condition AS o
WHERE o.keep = 1 AND o.d = o.n
SETTINGS optimize_correlated_scalar_aggregate_to_window = 0;

SELECT o.d, (SELECT max(v) FROM t_correlated_throwing_key_condition AS s WHERE s.d = o.d AND s.n = o.n)
FROM t_correlated_throwing_key_condition AS o
WHERE o.keep = 1 AND o.d = o.n
SETTINGS optimize_correlated_scalar_aggregate_to_window = 1;

DROP TABLE t_correlated_throwing_key_condition;
