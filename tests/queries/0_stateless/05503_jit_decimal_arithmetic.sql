-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `plus`, `minus`, `multiply` and `divide` of two `Decimal` values are compiled, with `decimal_check_overflow` on (errors)
-- and off (wrap-around). Each query runs interpreted and compiled, and both must give the same result or error. Each
-- operation is the argument of `identity`, which is compiled for `Decimal`, so that it is compiled; `query_log` shows that
-- the compiled queries ran compiled, and that the overflow errors came from compiled code.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_decimal_arithmetic;
CREATE TABLE t_jit_decimal_arithmetic
(
    k UInt8, a Decimal32(2), b Decimal32(4), c Decimal64(2), d Decimal64(6), e Decimal128(2), f Decimal128(4), ten Decimal64(0)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_jit_decimal_arithmetic VALUES
    (0, 0, 0, 0, 0, 0, 0, 10),
    (1, 1.5, -0.25, 1000.5, 0.000001, 12345678.9, -0.0001, 10),
    (2, -9999999.99, 99999.9999, -99999999999.99, -999999999999.999999, -1, 99999999999999999999999999999999.9999, 10),
    (3, 9999999.99, -99999.9999, 99999999999.99, 999999999999.999999, 1, -99999999999999999999999999999999.9999, 10),
    (4, 0.01, 0.0001, 0.01, 0.000001, 0.01, 0.0001, 10);

SELECT 'no overflow';
SELECT k, identity(a + b), identity(c - a), identity(c * a), identity(e + a), identity(e - c), identity(e + f) FROM t_jit_decimal_arithmetic WHERE k IN (0, 1, 4) ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(a + b), identity(c - a), identity(c * a), identity(e + a), identity(e - c), identity(e + f) FROM t_jit_decimal_arithmetic WHERE k IN (0, 1, 4) ORDER BY k SETTINGS compile_expressions = 1, log_comment = '05503_no_overflow';
SELECT k, identity(c / a), identity(d / c) FROM t_jit_decimal_arithmetic WHERE k IN (1, 4) ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(c / a), identity(d / c) FROM t_jit_decimal_arithmetic WHERE k IN (1, 4) ORDER BY k SETTINGS compile_expressions = 1, log_comment = '05503_division';

SELECT 'overflow, checked';
SELECT identity(a * b) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT identity(a * b) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 1, log_comment = '05503_overflow_multiply'; -- { serverError DECIMAL_OVERFLOW }
SELECT identity(d * ten) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT identity(d * ten) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 1, log_comment = '05503_overflow_multiply_64'; -- { serverError DECIMAL_OVERFLOW }
SELECT identity(d + d * ten) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT identity(d + d * ten) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 1, log_comment = '05503_overflow_plus'; -- { serverError DECIMAL_OVERFLOW }

SELECT 'overflow, unchecked';
SELECT k, identity(a * b), identity(d * ten), identity(d - c), identity(e + f), identity(e - f) FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 0, decimal_check_overflow = 0;
SELECT k, identity(a * b), identity(d * ten), identity(d - c), identity(e + f), identity(e - f) FROM t_jit_decimal_arithmetic ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 0, log_comment = '05503_unchecked';

SELECT 'division by zero';
SELECT identity(c / a) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 0; -- { serverError ILLEGAL_DIVISION }
SELECT identity(c / a) FROM t_jit_decimal_arithmetic SETTINGS compile_expressions = 1, log_comment = '05503_division_by_zero'; -- { serverError ILLEGAL_DIVISION }

SYSTEM FLUSH LOGS query_log;
SELECT 'compiled';
SELECT log_comment, ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05503_%' AND log_comment NOT LIKE '%.sql%' AND event_date >= yesterday()
ORDER BY log_comment;
SELECT 'errors from compiled code';
SELECT log_comment, exception LIKE '%while executing JIT-compiled%' FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND log_comment LIKE '05503_%' AND log_comment NOT LIKE '%.sql%' AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_jit_decimal_arithmetic;
