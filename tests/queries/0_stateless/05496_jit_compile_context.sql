-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Compiled expressions are cached by their functions and types, and by the settings each function captured
-- (`IFunctionBase::updateHash`). The same expression run with different settings, one after the other, must give the
-- result of each setting, never the compiled code built for another one. The expressions cover functions whose result
-- depends on a setting or on the session time zone, whether they are compiled yet or not.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_context;
CREATE TABLE t_jit_context (k UInt8, d Decimal32(2), dt Date, ts DateTime, n Nullable(Int32), b Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_context VALUES (0, 9999999.99, '2149-06-05', '2026-03-29 01:30:00', NULL, 1), (1, -1.5, '1970-01-02', '2026-10-25 02:30:00', 3, 0);

SELECT 'decimal_check_overflow';
SELECT k, d * d + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 0;
SELECT k, d * d + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 1; -- { serverError DECIMAL_OVERFLOW }
SELECT k, d * d + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, decimal_check_overflow = 0;
SELECT k, d * d + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, decimal_check_overflow = 0;

SELECT 'date_time_overflow_behavior';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, date_time_overflow_behavior = 'ignore';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, date_time_overflow_behavior = 'saturate';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, date_time_overflow_behavior = 'throw';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, date_time_overflow_behavior = 'ignore';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, date_time_overflow_behavior = 'saturate';
SELECT k, dt + 2 > dt FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, date_time_overflow_behavior = 'throw';

SELECT 'session_timezone';
SELECT k, toHour(ts) + 1, toStartOfDay(ts) < ts FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, session_timezone = 'Europe/Berlin';
SELECT k, toHour(ts) + 1, toStartOfDay(ts) < ts FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, session_timezone = 'Asia/Kolkata';
SELECT k, toHour(ts) + 1, toStartOfDay(ts) < ts FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, session_timezone = 'Europe/Berlin';
SELECT k, toHour(ts) + 1, toStartOfDay(ts) < ts FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, session_timezone = 'Asia/Kolkata';

SELECT 'least_greatest_legacy_null_behavior';
SELECT k, greatest(n, b) * 2, least(n, b, k) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 1;
SELECT k, greatest(n, b) * 2, least(n, b, k) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, least_greatest_legacy_null_behavior = 0;
SELECT k, greatest(n, b) * 2, least(n, b, k) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, least_greatest_legacy_null_behavior = 1;
SELECT k, greatest(n, b) * 2, least(n, b, k) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, least_greatest_legacy_null_behavior = 0;

SELECT 'short_circuit_function_evaluation';
SELECT k, if(b != 0, intDiv(10, b), 0) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, short_circuit_function_evaluation = 'enable';
SELECT k, if(b != 0, intDiv(10, b), 0) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }
SELECT k, if(b != 0, intDiv(10, b), 0) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 1, short_circuit_function_evaluation = 'force_enable';
SELECT k, if(b != 0, intDiv(10, b), 0) + 1 FROM t_jit_context ORDER BY k SETTINGS compile_expressions = 0, short_circuit_function_evaluation = 'enable';

DROP TABLE t_jit_context;
