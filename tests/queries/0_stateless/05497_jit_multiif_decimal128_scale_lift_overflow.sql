-- A compiled `multiIf` / `CASE` that lifts a `Decimal` branch to a larger scale of a `Decimal128` result raises
-- `DECIMAL_OVERFLOW` like the interpreted cast when the lifted value does not fit.
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT multiIf(number = 0, materialize(toDecimal128(1000000000, 0)), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal64(1000000000, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 30)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal32(999999999, 0)), number = 1, toDecimal32(1, 0), toDecimal128(1, 30)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toNullable(toDecimal128(1000000000, 0))), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }

DROP TABLE IF EXISTS t_jit_decimal128_lift;
CREATE TABLE t_jit_decimal128_lift (k UInt8, d Decimal(38, 0), e Decimal(38, 30)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_decimal128_lift VALUES (0, 1000000000, 1), (1, 5, 2), (2, -170141184, 3);
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal128_lift WHERE k = 1 ORDER BY k;
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal128_lift ORDER BY k; -- { serverError DECIMAL_OVERFLOW }
DROP TABLE t_jit_decimal128_lift;

-- The widest lifts that cannot overflow stay compiled and exact at the lowest value; one step wider is interpreted.
SELECT multiIf(number = 0, materialize(toDecimal64(-9223372036854775808, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 19)) FROM numbers(1);
SELECT multiIf(number = 0, materialize(toDecimal32(-2147483648, 0)), number = 1, toDecimal32(1, 0), toDecimal128(1, 28)) FROM numbers(1);
SELECT multiIf(number = 0, materialize(toDecimal64(1, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 19)) FROM numbers(2)
    SETTINGS log_comment = '05497_d64_kept' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal32(1, 0)), number = 1, toDecimal32(1, 0), toDecimal128(1, 28)) FROM numbers(2)
    SETTINGS log_comment = '05497_d32_kept' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal64(1, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 20)) FROM numbers(2)
    SETTINGS log_comment = '05497_d64_declined' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal32(1, 0)), number = 1, toDecimal32(1, 0), toDecimal128(1, 29)) FROM numbers(2)
    SETTINGS log_comment = '05497_d32_declined' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal128(1, 2)), number = 1, toDecimal128(1, 2), toDecimal128(1, 3)) FROM numbers(2)
    SETTINGS log_comment = '05497_d128_declined' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal128(1, 3)), number = 1, toDecimal128(1, 3), toDecimal128(2, 3)) FROM numbers(2)
    SETTINGS log_comment = '05497_same_scale_kept' FORMAT Null;
SELECT materialize(2.0) + materialize(0.0) + materialize(1.0) FROM numbers(2)
    SETTINGS log_comment = '05497_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05497_%'
    GROUP BY log_comment
)
-- Comparing with the control keeps the kept columns valid in a build without the embedded compiler.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05497_d64_kept') = (SELECT compiled FROM shapes WHERE log_comment = '05497_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05497_d32_kept') = (SELECT compiled FROM shapes WHERE log_comment = '05497_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05497_same_scale_kept') = (SELECT compiled FROM shapes WHERE log_comment = '05497_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05497_d64_declined') = 0,
    (SELECT compiled FROM shapes WHERE log_comment = '05497_d32_declined') = 0,
    (SELECT compiled FROM shapes WHERE log_comment = '05497_d128_declined') = 0;
