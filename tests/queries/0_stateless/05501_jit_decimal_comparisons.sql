-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Comparisons of `Decimal` values of the same scale are compiled, as comparisons of the integers they store; values of
-- different scales stay interpreted. Each query runs interpreted and compiled, and both must give the same result. Each
-- comparison is fused with `not`, so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_decimal_compare;
CREATE TABLE t_jit_decimal_compare
(
    k UInt8, a Decimal32(2), b Decimal64(2), c Decimal128(2), d Decimal32(2), e Decimal64(4), n Nullable(Decimal64(2))
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_jit_decimal_compare VALUES
    (0, 0, 0, 0, 0, 0, NULL),
    (1, -9999999.99, -9999999.99, -9999999.99, 9999999.99, -1.5, -9999999.99),
    (2, 9999999.99, 99999999999999.99, -99999999999999999999.99, -0.01, 1.5, 0.01),
    (3, 0.01, 0.01, 0.02, 0.01, 0.0001, 0.01),
    (4, -0.01, -0.02, -0.01, -0.01, -0.0001, NULL);

SELECT 'same scale';
SELECT k, not(a = b), not(a < c), not(b >= c), not(a != d), not(n <= a), not(c > n) FROM t_jit_decimal_compare ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(a = b), not(a < c), not(b >= c), not(a != d), not(n <= a), not(c > n) FROM t_jit_decimal_compare ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'different scales';
SELECT k, not(b < e), not(a = e) FROM t_jit_decimal_compare ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(b < e), not(a = e) FROM t_jit_decimal_compare ORDER BY k SETTINGS compile_expressions = 1;

-- The comparisons of the same scale ran compiled.
SELECT not(a = b), not(a < c), not(b >= c) FROM t_jit_decimal_compare
    SETTINGS compile_expressions = 1, log_comment = '05501_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'] = 6 * 5, ProfileEvents['InterpretedFunctionRows'] = 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05501_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_decimal_compare;
