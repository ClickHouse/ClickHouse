-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- A `Date` compared with a `DateTime` is converted to the `DateTime` as the interpreter does (`castColumn`: the day limited to
-- 2106-02-06, the start of the day in the time zone of the `DateTime`, at least the first day that starts after the epoch),
-- whatever `date_time_overflow_behavior` is. Each query runs interpreted and compiled, and both must give the same result.
-- Each comparison is the argument of `identity`, so that it is compiled.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_date_datetime;
CREATE TABLE t_jit_date_datetime (d Date, t_utc DateTime('UTC'), t_kolkata DateTime('Asia/Kolkata'), t_ny DateTime('America/New_York'), t DateTime)
ENGINE = MergeTree ORDER BY d;
INSERT INTO t_jit_date_datetime
SELECT d, t, t, t, t
FROM (SELECT arrayJoin([toDate('1970-01-01'), toDate('1970-01-02'), toDate('2020-03-08'), toDate('2106-02-06'), toDate('2106-02-07'), toDate('2149-06-06')]) AS d) AS dates
CROSS JOIN (SELECT arrayJoin([0, 1, 18000, 19799, 19800, 66599, 66600, 86400, 1583625600, 1583643600, 4294837800, 4294857600, 4294875600, 4294944000, 4294967295]::Array(UInt32)) AS t) AS times;

SELECT 'UTC, Asia/Kolkata, America/New_York';
SELECT sum(cityHash64(d, t_utc, identity(d < t_utc), identity(d = t_utc), identity(t_utc <= d), identity(d < t_kolkata), identity(t_kolkata = d), identity(d >= t_kolkata), identity(d < t_ny), identity(d = t_ny), identity(t_ny > d))),
    countIf(identity(d < t_utc)), countIf(identity(d = t_kolkata)), countIf(identity(d = t_ny)), countIf(identity(t_ny > d))
FROM t_jit_date_datetime SETTINGS compile_expressions = 0;
SELECT sum(cityHash64(d, t_utc, identity(d < t_utc), identity(d = t_utc), identity(t_utc <= d), identity(d < t_kolkata), identity(t_kolkata = d), identity(d >= t_kolkata), identity(d < t_ny), identity(d = t_ny), identity(t_ny > d))),
    countIf(identity(d < t_utc)), countIf(identity(d = t_kolkata)), countIf(identity(d = t_ny)), countIf(identity(t_ny > d))
FROM t_jit_date_datetime SETTINGS compile_expressions = 1;

SELECT 'overflow behavior does not apply';
SELECT countIf(identity(d < t_utc)), countIf(identity(d = t_kolkata)) FROM t_jit_date_datetime SETTINGS compile_expressions = 1, date_time_overflow_behavior = 'throw';
SELECT countIf(identity(d < t_utc)), countIf(identity(d = t_kolkata)) FROM t_jit_date_datetime SETTINGS compile_expressions = 1, date_time_overflow_behavior = 'saturate';

SELECT 'DateTime without a time zone';
-- It holds the session time zone of the creation of the table, which depends on the environment, so only the equality
-- is printed. Compiled code for it is keyed by that time zone, see `05507_jit_date_time_zone_of_type`.
SELECT (SELECT groupArray((identity(d < t), identity(d = t), identity(t >= d))) FROM (SELECT d, t FROM t_jit_date_datetime ORDER BY d, t) SETTINGS compile_expressions = 0) = (SELECT groupArray((identity(d < t), identity(d = t), identity(t >= d))) FROM (SELECT d, t FROM t_jit_date_datetime ORDER BY d, t) SETTINGS compile_expressions = 1);

SELECT 'constant Date';
SELECT countIf(identity(toDate('2020-03-08') < t_kolkata)), countIf(identity(t_ny = toDate('1970-01-02'))), countIf(identity(toDate('2149-06-06') >= t_utc)) FROM t_jit_date_datetime SETTINGS compile_expressions = 0;
SELECT countIf(identity(toDate('2020-03-08') < t_kolkata)), countIf(identity(t_ny = toDate('1970-01-02'))), countIf(identity(toDate('2149-06-06') >= t_utc)) FROM t_jit_date_datetime SETTINGS compile_expressions = 1;

-- The compiled queries ran compiled: three `identity` of a comparison, 6 functions on 90 rows.
SELECT identity(d < t_utc), identity(t_kolkata = d), identity(d >= t_ny) FROM t_jit_date_datetime
    SETTINGS compile_expressions = 1, log_comment = '05506_compiled' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CompiledFunctionRows'], ProfileEvents['InterpretedFunctionRows'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05506_compiled' AND event_date >= yesterday();

DROP TABLE t_jit_date_datetime;
