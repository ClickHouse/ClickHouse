-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- A `DateTime` column without a time zone of its own holds the session time zone of its creation, and is named `DateTime`
-- whatever it is. The same compiled expression over two such columns of different time zones must read each one's own
-- time-zone table: compiled code must not be shared between them.

SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_tz_kolkata;
DROP TABLE IF EXISTS t_jit_tz_los_angeles;
SET session_timezone = 'Asia/Kolkata';
CREATE TABLE t_jit_tz_kolkata (t DateTime) ENGINE = Memory;
SET session_timezone = 'America/Los_Angeles';
CREATE TABLE t_jit_tz_los_angeles (t DateTime) ENGINE = Memory;
SET session_timezone = 'UTC';
INSERT INTO t_jit_tz_kolkata VALUES (1700000000);
INSERT INTO t_jit_tz_los_angeles VALUES (1700000000);

SELECT timezoneOf(t), identity(toYYYYMMDD(t)), identity(toYYYYMM(t)) FROM t_jit_tz_kolkata SETTINGS compile_expressions = 1;
SELECT timezoneOf(t), identity(toYYYYMMDD(t)), identity(toYYYYMM(t)) FROM t_jit_tz_los_angeles SETTINGS compile_expressions = 1;
SELECT timezoneOf(t), identity(toYYYYMMDD(t)), identity(toYYYYMM(t)) FROM t_jit_tz_los_angeles SETTINGS compile_expressions = 0;

DROP TABLE t_jit_tz_kolkata;
DROP TABLE t_jit_tz_los_angeles;
