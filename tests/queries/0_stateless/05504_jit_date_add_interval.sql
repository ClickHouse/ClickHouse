-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `addSeconds`, `addMinutes`, `addHours` of a `DateTime`, `addDays`, `addWeeks` of a `Date` or a `Date32`, their `subtract*`
-- counterparts, and `+`/`-` of an integer (through those functions), are compiled as the wrap-around arithmetic of the interpreter. Each query runs
-- interpreted and compiled, and both must give the same result. Each function is the argument of `identity`, so that it is
-- compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_add_interval;
CREATE TABLE t_jit_add_interval (k UInt8, t DateTime('UTC'), d Date, d32 Date32, n Int64, s Int8, u UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_add_interval VALUES
    (0, 0, '1970-01-01', '1900-01-01', 0, 0, 0),
    (1, 4294967295, '2149-06-06', '2299-12-31', 1, -1, 4294967295),
    (2, 1234567890, '2020-02-29', '1969-12-31', -1, 127, 1),
    (3, 86399, '2000-01-01', '1950-06-15', 9223372036854775807, -128, 86400),
    (4, 1700000000, '1999-12-31', '2100-01-01', -9223372036854775808, 3, 65535);

SELECT 'DateTime';
SELECT k, identity(addSeconds(t, n)), identity(addMinutes(t, s)), identity(addHours(t, u)), identity(subtractSeconds(t, n)), identity(subtractHours(t, s)) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(addSeconds(t, n)), identity(addMinutes(t, s)), identity(addHours(t, u)), identity(subtractSeconds(t, n)), identity(subtractHours(t, s)) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'Date';
SELECT k, identity(addDays(d, n)), identity(addWeeks(d, s)), identity(subtractDays(d, u)), identity(addDays(d32, n)), identity(subtractWeeks(d32, s)), identity(addWeeks(d32, u)) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(addDays(d, n)), identity(addWeeks(d, s)), identity(subtractDays(d, u)), identity(addDays(d32, n)), identity(subtractWeeks(d32, s)), identity(addWeeks(d32, u)) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 1;

SELECT 'plus and minus';
SELECT k, identity(d + s), identity(t - s), identity(d32 - n), identity(t + INTERVAL 1 HOUR), identity(d - INTERVAL 2 WEEK) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(d + s), identity(t - s), identity(d32 - n), identity(t + INTERVAL 1 HOUR), identity(d - INTERVAL 2 WEEK) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 1;
SELECT k, identity(s + d32), identity(u + t), identity(n + d) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(s + d32), identity(u + t), identity(n + d) FROM t_jit_add_interval ORDER BY k SETTINGS compile_expressions = 1;

-- The compiled queries ran compiled: five `identity` of a compiled function, 10 functions on 5 rows. `+`/`-` of an `INTERVAL`
-- constant stays interpreted (an `Interval` is not a native value).
SELECT identity(addSeconds(t, n)), identity(addDays(d, n)), identity(d + s), identity(s + d32), identity(t - s) FROM t_jit_add_interval
    SETTINGS compile_expressions = 1, log_comment = '05504_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'], ProfileEvents['InterpretedFunctionRows'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05504_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_add_interval;
