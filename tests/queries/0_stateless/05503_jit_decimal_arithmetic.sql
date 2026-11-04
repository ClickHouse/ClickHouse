-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `plus`, `minus`, `multiply` and `divide` of two `Decimal` values are compiled, with `decimal_check_overflow` on (errors)
-- and off (wrap-around). Each query runs interpreted and compiled, and both must give the same result or error. Each
-- operation is fused with a comparison, so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_decimal_arithmetic;
CREATE TABLE t_jit_decimal_arithmetic
(
    k UInt8, a Decimal32(2), b Decimal32(4), c Decimal64(2), d Decimal64(6), e Decimal128(2)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_jit_decimal_arithmetic VALUES
    (0, 0, 0, 0, 0, 0),
    (1, 1.5, -0.25, 1000.5, 0.000001, 12345678.9),
    (2, -9999999.99, 99999.9999, -99999999999.99, -999999999999.999999, -1),
    (3, 9999999.99, -99999.9999, 99999999999.99, 999999999999.999999, 1),
    (4, 0.01, 0.0001, 0.01, 0.000001, 0.01);

SELECT 'no overflow';
SELECT k, a + b > 0, c - a < 1, a * b = 0, c * a > 1, e + a > 0, e - c < 0 FROM t_jit_decimal_arithmetic WHERE k != 2 AND k != 3 ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, a + b > 0, c - a < 1, a * b = 0, c * a > 1, e + a > 0, e - c < 0 FROM t_jit_decimal_arithmetic WHERE k != 2 AND k != 3 ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, (c / a) > 1, (d / c) < 0 FROM t_jit_decimal_arithmetic WHERE k = 1 OR k = 4 ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, (c / a) > 1, (d / c) < 0 FROM t_jit_decimal_arithmetic WHERE k = 1 OR k = 4 ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'overflow, checked';
SELECT k, a * b > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT k, a * b > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }
SELECT k, d * materialize(toDecimal64(10, 0)) > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT k, d * materialize(toDecimal64(10, 0)) > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }
SELECT k, (d * materialize(toDecimal64(10, 0))) - d > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT k, (d * materialize(toDecimal64(10, 0))) - d > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }
SELECT materialize(toDecimal64(9000000000000, 6)) + materialize(toDecimal64(9000000000000, 6)) > 0 SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT materialize(toDecimal64(9000000000000, 6)) + materialize(toDecimal64(9000000000000, 6)) > 0 SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }

SELECT 'overflow, unchecked';
SELECT k, a * b, d * materialize(toDecimal64(10, 0)), d - c FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0, decimal_check_overflow = 0;
SELECT k, a * b, d * materialize(toDecimal64(10, 0)), d - c FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 0;
SELECT k, (a * b) > 0, (d * materialize(toDecimal64(10, 0))) < 0, (d - c) > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0, decimal_check_overflow = 0;
SELECT k, (a * b) > 0, (d * materialize(toDecimal64(10, 0))) < 0, (d - c) > 0 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 0;

SELECT 'division by zero';
SELECT k, (c / a) > 1 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0; -- { serverError ILLEGAL_DIVISION }
SELECT k, (c / a) > 1 FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1; -- { serverError ILLEGAL_DIVISION }

-- The compiled queries ran compiled.
SELECT a + b > 0, c - a < 1, a * b = 0 FROM t_jit_decimal_arithmetic WHERE k != 2 AND k != 3
    SETTINGS compile_expressions = 1, log_comment = '05503_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05503_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_decimal_arithmetic;
