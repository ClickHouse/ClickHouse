-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Compiled comparisons of 128-bit integers, of `UInt64` with signed integers, of floats with 64-bit integers and of `Enum`
-- values; conversions of `Nullable` arguments; `least` and `greatest` with three or more or `Nullable` arguments.
-- Each query runs interpreted and compiled, and both must give the same result. Each function is fused with another one
-- (`not`, `plus`), so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_gaps;
CREATE TABLE t_jit_gaps
(
    k UInt8,
    u64 UInt64, i64 Int64, i8 Int8, i128 Int128, u128 UInt128,
    f64 Float64, f32 Float32,
    e Enum8('a' = -1, 'b' = 0, 'c' = 1),
    n Nullable(Int32), m Nullable(Float64)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_jit_gaps VALUES
    (0, 0, 0, 0, 0, 0, 0, 0, 'a', NULL, NULL),
    (1, 18446744073709551615, -1, -1, -1, 340282366920938463463374607431768211455, 9223372036854775807, 9223372036854775807, 'b', 1, nan),
    (2, 9223372036854775808, -9223372036854775808, -128, -170141183460469231731687303715884105728, 1, -9223372036854775808, -9223372036854775808, 'c', -5, 2),
    (3, 9223372036854775807, 9223372036854775807, 127, 170141183460469231731687303715884105727, 170141183460469231731687303715884105728, 9223372036854775808, 1e19, 'a', 7, -inf),
    (4, 9007199254740993, 9007199254740993, 1, 18446744073709551616, 18446744073709551615, 9007199254740992, 16777217, 'b', 0, 0.5),
    (5, 1, -9007199254740993, 2, -18446744073709551616, 2, -9007199254740992, -0.5, 'c', NULL, -0);

SELECT 'UInt64 with signed';
SELECT k, not(u64 > i64), not(u64 = i8), not(i64 <= u64), not(u64 != i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(u64 > i64), not(u64 = i8), not(i64 <= u64), not(u64 != i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;

SELECT '128-bit integers';
SELECT k, not(i128 < u128), not(i128 = i64), not(u128 >= u64), not(i128 > i8), not(u128 = i128) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(i128 < u128), not(i128 = i64), not(u128 >= u64), not(i128 > i8), not(u128 = i128) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'float with Int64 and UInt64';
SELECT k, not(f64 = i64), not(f64 < i64), not(i64 >= f64), not(f32 > u64), not(u64 = f64), not(f64 != i64), not(f32 <= i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(f64 = i64), not(f64 < i64), not(i64 >= f64), not(f32 > u64), not(u64 = f64), not(f64 != i64), not(f32 <= i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, not(m = i64), not(m < u64), not(m != i64), not(m >= i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(m = i64), not(m < u64), not(m != i64), not(m >= i64) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'Enum';
SELECT k, not(e < materialize(CAST('c' AS Enum8('a' = -1, 'b' = 0, 'c' = 1)))), not(e = e), not(e >= materialize(CAST('b' AS Enum8('a' = -1, 'b' = 0, 'c' = 1)))) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, not(e < materialize(CAST('c' AS Enum8('a' = -1, 'b' = 0, 'c' = 1)))), not(e = e), not(e >= materialize(CAST('b' AS Enum8('a' = -1, 'b' = 0, 'c' = 1)))) FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'Nullable conversions';
SELECT k, toUInt32(n) + 1, toInt8(n) + 1, toFloat64(n) + 1, toUInt64(n + 1) + 1, toInt16(n) * 2 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, toUInt32(n) + 1, toInt8(n) + 1, toFloat64(n) + 1, toUInt64(n + 1) + 1, toInt16(n) * 2 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'least and greatest';
SELECT k, least(i8, n, k) + 1, greatest(n, i8) + 1, least(m, f64, f32) + 1, greatest(m, f64) + 1, greatest(f32, m, 0) + 1, least(n, n) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, least(i8, n, k) + 1, greatest(n, i8) + 1, least(m, f64, f32) + 1, greatest(m, f64) + 1, greatest(f32, m, 0) + 1, least(n, n) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, least(i8, n, k) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 1;
SELECT k, least(i8, n, k) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0, least_greatest_legacy_null_behavior = 1;

-- With two `Nullable` numbers the legacy NULL behavior builds another `least` of the same name and types; the two must
-- not share compiled code, whichever runs first.
SELECT 'two Nullable arguments, both NULL behaviors';
SELECT k, least(n, i8) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 0;
SELECT k, least(n, i8) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 1;
SELECT k, least(n, i8) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 0;
SELECT k, least(n, i8) + 1 FROM t_jit_gaps ORDER BY k SETTINGS compile_expressions = 0, least_greatest_legacy_null_behavior = 1;

-- The compiled queries ran compiled; with the legacy NULL behavior `least` stays interpreted.
SELECT not(u64 > i64), not(f64 = i64), toUInt32(n) + 1, least(i8, n, k) + 1 FROM t_jit_gaps
    SETTINGS compile_expressions = 1, log_comment = '05495_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'] >= 8 * 6 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05495_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_gaps;
