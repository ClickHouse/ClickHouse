-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `coalesce`, `ifNull`, `nullIf`, `intDivOrZero`, `moduloOrZero`, `intDivOrNull`, `moduloOrNull`, `divideOrNull` and
-- `modulo` on floats are compiled. Each query runs interpreted and compiled, and both must give the same result. Each
-- function is fused with another one, so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_null_division;
CREATE TABLE t_jit_null_division
(
    k UInt8, a Int32, b Int32, u UInt8, f Float64, g Float32,
    n Nullable(Int32), m Nullable(Int64), x Nullable(Float64)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_jit_null_division VALUES
    (0, 10, 3, 255, 7.5, 2, NULL, NULL, NULL),
    (1, -2147483648, -1, 0, -7.5, -0, 5, -9223372036854775808, nan),
    (2, 7, 0, 1, inf, 0.5, NULL, 3, -0),
    (3, -9, 2, 128, -0, -3, -1, NULL, inf),
    (4, 0, -3, 2, 1e308, 1e-30, 0, 0, 2.5);

SELECT 'coalesce and ifNull';
SELECT k, coalesce(n, m, b) + 1, coalesce(n, x) * 2, coalesce(n, m) + 1, ifNull(n, b) + 1, ifNull(x, f) * 2, ifNull(a, b) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, coalesce(n, m, b) + 1, coalesce(n, x) * 2, coalesce(n, m) + 1, ifNull(n, b) + 1, ifNull(x, f) * 2, ifNull(a, b) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, assumeNotNull(coalesce(n, m)) + 1, coalesce(u, n) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, assumeNotNull(coalesce(n, m)) + 1, coalesce(u, n) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'nullIf';
SELECT k, nullIf(a, b) + 1, nullIf(n, 5) + 1, nullIf(b, n) + 1, nullIf(f, g) * 2, assumeNotNull(nullIf(a, -2147483648)) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, nullIf(a, b) + 1, nullIf(n, 5) + 1, nullIf(b, n) + 1, nullIf(f, g) * 2, assumeNotNull(nullIf(a, -2147483648)) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'OrZero';
SELECT k, intDivOrZero(a, b) + 1, moduloOrZero(a, b) + 1, intDivOrZero(a, u) + 1, moduloOrZero(u, b) + 1, moduloOrZero(f, g) * 2 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, intDivOrZero(a, b) + 1, moduloOrZero(a, b) + 1, intDivOrZero(a, u) + 1, moduloOrZero(u, b) + 1, moduloOrZero(f, g) * 2 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'OrNull';
SELECT k, intDivOrNull(a, b) + 1, moduloOrNull(a, b) + 1, intDivOrNull(n, b) + 1, intDivOrNull(a, n) + 1, divideOrNull(f, b) * 2, divideOrNull(a, x) * 2, moduloOrNull(f, g) * 2, moduloOrNull(m, n) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, intDivOrNull(a, b) + 1, moduloOrNull(a, b) + 1, intDivOrNull(n, b) + 1, intDivOrNull(a, n) + 1, divideOrNull(f, b) * 2, divideOrNull(a, x) * 2, moduloOrNull(f, g) * 2, moduloOrNull(m, n) + 1 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'modulo on floats';
SELECT k, modulo(f, g) * 2, modulo(a, f) + 1, modulo(g, 3) - 1, modulo(x, f) * 2, modulo(a, x) * 2, moduloOrZero(f, x) * 2 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, modulo(f, g) * 2, modulo(a, f) + 1, modulo(g, 3) - 1, modulo(x, f) * 2, modulo(a, x) * 2, moduloOrZero(f, x) * 2 FROM t_jit_null_division ORDER BY k SETTINGS compile_expressions = 1;

-- The compiled queries ran compiled.
SELECT coalesce(n, m, b) + 1, ifNull(n, b) + 1, nullIf(a, b) + 1, intDivOrZero(a, b) + 1, intDivOrNull(a, b) + 1, modulo(f, g) * 2
FROM t_jit_null_division SETTINGS compile_expressions = 1, log_comment = '05498_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'] >= 12 * 5, ProfileEvents['InterpretedFunctionRows'] = 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05498_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_null_division;
