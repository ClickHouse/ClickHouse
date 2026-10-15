-- `max_rows_to_read`, `max_bytes_to_read` and `max_rows_to_read_leaf` apply to `Memory`, `Log` and `TinyLog` tables.

DROP TABLE IF EXISTS t_read_limits_memory;
DROP TABLE IF EXISTS t_read_limits_memory_compressed;
DROP TABLE IF EXISTS t_read_limits_log;
DROP TABLE IF EXISTS t_read_limits_tinylog;

CREATE TABLE t_read_limits_memory (k UInt64) ENGINE = Memory;
CREATE TABLE t_read_limits_memory_compressed (k UInt64) ENGINE = Memory SETTINGS compress = 1;
CREATE TABLE t_read_limits_log (k UInt64) ENGINE = Log;
CREATE TABLE t_read_limits_tinylog (k UInt64) ENGINE = TinyLog;
CREATE TEMPORARY TABLE t_read_limits_temporary (k UInt64);

INSERT INTO t_read_limits_memory SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_memory_compressed SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_log SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_tinylog SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_temporary SELECT number FROM numbers(1000);

SELECT sum(k) FROM t_read_limits_memory SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_memory SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_memory SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT k FROM t_read_limits_memory PREWHERE k % 2 = 0 ORDER BY k LIMIT 1 SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_memory SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_memory_compressed SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_memory_compressed SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_temporary SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_temporary SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read = 1000;

DROP TABLE t_read_limits_memory;
DROP TABLE t_read_limits_memory_compressed;
DROP TABLE t_read_limits_log;
DROP TABLE t_read_limits_tinylog;
