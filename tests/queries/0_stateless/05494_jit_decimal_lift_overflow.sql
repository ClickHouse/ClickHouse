-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- A `multiIf`, `CASE` or `if` that lifts a `Decimal` branch to a larger result scale raises `DECIMAL_OVERFLOW` where the
-- interpreted cast does, instead of wrapping around in compiled code: https://github.com/ClickHouse/ClickHouse/issues/126516
-- A lift that can overflow is not compiled: the interpreter skips a branch, or stops, at a condition that is a constant
-- column at run time, which compiled code cannot know. Each query runs with the JIT off and on.

SET min_count_to_compile_expression = 0;

SELECT 'lift into Decimal128';
SELECT multiIf(number = 0, materialize(toDecimal128(1000000000, 0)), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) FROM numbers(1) SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal128(1000000000, 0)), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) FROM numbers(1) SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal64(1000000000, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 30)) FROM numbers(1) SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal64(1000000000, 0)), number = 1, toDecimal64(1, 0), toDecimal128(1, 30)) FROM numbers(1) SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }

SELECT 'values that fit';
SELECT multiIf(number = 0, materialize(toDecimal128(100000000, 0)), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) AS x, toTypeName(x) FROM numbers(3) SETTINGS compile_expressions = 0;
SELECT multiIf(number = 0, materialize(toDecimal128(100000000, 0)), number = 1, toDecimal128(1, 0), toDecimal128(1, 30)) AS x, toTypeName(x) FROM numbers(3) SETTINGS compile_expressions = 1;

DROP TABLE IF EXISTS t_jit_decimal_lift;
CREATE TABLE t_jit_decimal_lift (k UInt8, d Decimal(38, 0), e Decimal(38, 30), s Decimal(18, 2), n Nullable(Decimal(38, 0))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_decimal_lift VALUES (0, 1000000000, 1, 1, NULL), (1, 5, 2, 2, 5), (2, -170141184, 3, 3, -1000000000);

SELECT 'CASE';
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal_lift WHERE k = 1 ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal_lift WHERE k = 1 ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT k, CASE WHEN k = 1 THEN e WHEN k = 3 THEN e ELSE d END FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }

-- A row that does not take the branch fails too: the interpreter casts the whole branch column.
SELECT 'branch not taken';
SELECT sum(multiIf(k = 0, e, k = 5, e, d) * 1) FROM t_jit_decimal_lift WHERE k <= 1 SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT sum(multiIf(k = 0, e, k = 5, e, d) * 1) FROM t_jit_decimal_lift WHERE k <= 1 SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }

-- Smaller storage lifted into a larger one: the least supertype reserves the digits, so the lift cannot overflow.
SELECT 'Decimal64 into Decimal128';
SELECT k, multiIf(k = 0, s, k = 5, s, e) AS x, toTypeName(x) FROM t_jit_decimal_lift WHERE k <= 1 ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, multiIf(k = 0, s, k = 5, s, e) AS x, toTypeName(x) FROM t_jit_decimal_lift WHERE k <= 1 ORDER BY k SETTINGS compile_expressions = 1;

-- Nullable: the NULL row has a default nested value, which fits; the negative row does not.
SELECT 'Nullable';
SELECT k, multiIf(k = 1, n, k = 5, n, e) FROM t_jit_decimal_lift WHERE k <= 1 ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, multiIf(k = 1, n, k = 5, n, e) FROM t_jit_decimal_lift WHERE k <= 1 ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, multiIf(k = 1, n, k = 5, n, e) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT k, multiIf(k = 1, n, k = 5, n, e) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 1; -- { serverError DECIMAL_OVERFLOW }

-- A condition that is constant only at run time (`ignore` is not compiled) decides which branches are lifted.
SELECT 'condition constant at run time';
SELECT k, multiIf(k = 3, e, ignore(k) = 0, e, d) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, multiIf(k = 3, e, ignore(k) = 0, e, d) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, multiIf(ignore(k) = 1, d, k = 5, e, e) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, multiIf(ignore(k) = 1, d, k = 5, e, e) FROM t_jit_decimal_lift ORDER BY k SETTINGS compile_expressions = 1;

DROP TABLE t_jit_decimal_lift;
