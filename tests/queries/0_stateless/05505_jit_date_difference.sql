-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `Date - Date` and `DateTime - DateTime` are compiled as the `Int32` difference of the stored numbers, wrapping as the
-- interpreter does. Each query runs interpreted and compiled, and both must give the same result. Each difference is the
-- argument of `identity`, so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_date_difference;
CREATE TABLE t_jit_date_difference (k UInt8, t1 DateTime('UTC'), t2 DateTime('Asia/Kolkata'), d1 Date, d2 Date) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_date_difference VALUES
    (0, 0, 4294967295, '1970-01-01', '2149-06-06'),
    (1, 4294967295, 0, '2149-06-06', '1970-01-01'),
    (2, 1234567890, 1234567890, '2020-02-29', '2020-03-01'),
    (3, 2147483647, 4294967295, '2000-01-01', '1999-12-31'),
    (4, 2147483648, 0, '1999-12-31', '2000-01-01');

SELECT k, identity(t1 - t2), identity(t2 - t1), identity(t1 - t1), identity(d1 - d2), identity(d2 - d1), toTypeName(identity(d1 - d2)), toTypeName(identity(t1 - t2))
FROM t_jit_date_difference ORDER BY k SETTINGS compile_expressions = 0;
SELECT k, identity(t1 - t2), identity(t2 - t1), identity(t1 - t1), identity(d1 - d2), identity(d2 - d1), toTypeName(identity(d1 - d2)), toTypeName(identity(t1 - t2))
FROM t_jit_date_difference ORDER BY k SETTINGS compile_expressions = 1;

-- Fused with a comparison.
SELECT countIf(t1 - t2 > 0 AND d1 - d2 < 0) FROM t_jit_date_difference SETTINGS compile_expressions = 0;
SELECT countIf(t1 - t2 > 0 AND d1 - d2 < 0) FROM t_jit_date_difference SETTINGS compile_expressions = 1;

-- The compiled queries ran compiled: three `identity` of a difference, 6 functions on 5 rows.
SELECT identity(t1 - t2), identity(d1 - d2), identity(d2 - d1) FROM t_jit_date_difference
    SETTINGS compile_expressions = 1, log_comment = '05505_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'], ProfileEvents['InterpretedFunctionRows'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05505_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_date_difference;
