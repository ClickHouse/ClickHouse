-- `max_rows_to_read`, `max_bytes_to_read` and `max_rows_to_read_leaf` apply to `Memory`, `Log`, `TinyLog` and `StripeLog` tables.

DROP TABLE IF EXISTS t_read_limits_memory;
DROP TABLE IF EXISTS t_read_limits_memory_compressed;
DROP TABLE IF EXISTS t_read_limits_memory_small_blocks;
DROP TABLE IF EXISTS t_read_limits_memory_prewhere;
DROP TABLE IF EXISTS t_read_limits_log;
DROP TABLE IF EXISTS t_read_limits_tinylog;
DROP TABLE IF EXISTS t_read_limits_stripelog;

CREATE TABLE t_read_limits_memory (k UInt64) ENGINE = Memory;
CREATE TABLE t_read_limits_memory_compressed (k UInt64) ENGINE = Memory SETTINGS compress = 1;
CREATE TABLE t_read_limits_memory_small_blocks (k UInt64) ENGINE = Memory;
CREATE TABLE t_read_limits_memory_prewhere (k UInt64, x UInt8) ENGINE = Memory;
CREATE TABLE t_read_limits_log (k UInt64) ENGINE = Log;
CREATE TABLE t_read_limits_tinylog (k UInt64) ENGINE = TinyLog;
CREATE TABLE t_read_limits_stripelog (k UInt64) ENGINE = StripeLog;
CREATE TEMPORARY TABLE t_read_limits_temporary (k UInt64);

INSERT INTO t_read_limits_memory SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_memory_compressed SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_memory_small_blocks SELECT number FROM numbers(1000) SETTINGS max_block_size = 10, max_insert_block_size = 10, min_insert_block_size_rows = 10;
INSERT INTO t_read_limits_memory_prewhere SELECT number, number = 999 FROM numbers(1000) SETTINGS max_block_size = 10, max_insert_block_size = 10, min_insert_block_size_rows = 10, max_threads = 1, max_insert_threads = 1;
INSERT INTO t_read_limits_log SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_tinylog SELECT number FROM numbers(1000);
INSERT INTO t_read_limits_stripelog SELECT number FROM numbers(1000);
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

-- A trivial `LIMIT` caps the estimate of rows to read. `Memory` returns stored blocks whole, so they have to be smaller than the row limit.
SELECT count() FROM (SELECT k FROM t_read_limits_memory_small_blocks LIMIT 5) SETTINGS max_rows_to_read = 100;
SELECT count() FROM (SELECT k FROM t_read_limits_memory_small_blocks LIMIT 5) SETTINGS max_rows_to_read_leaf = 100;
SELECT count() FROM (SELECT k FROM t_read_limits_memory_small_blocks LIMIT 200) SETTINGS max_rows_to_read = 100; -- { serverError TOO_MANY_ROWS }

-- The limits are checked after a block whose rows are all rejected by `PREWHERE`, so the read stops before the only matching row.
-- A single insert thread keeps that row in the last stored block.
SELECT count() FROM t_read_limits_memory_prewhere PREWHERE x = 1 SETTINGS max_threads = 1, use_query_condition_cache = 0, max_rows_to_read = 100, read_overflow_mode = 'break';

SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_log SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_tinylog SETTINGS max_rows_to_read = 1000;

SELECT sum(k) FROM t_read_limits_stripelog SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_stripelog SETTINGS max_bytes_to_read = 100; -- { serverError TOO_MANY_BYTES }
SELECT sum(k) FROM t_read_limits_stripelog SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(k) FROM t_read_limits_stripelog SETTINGS max_rows_to_read = 1000;

DROP TABLE t_read_limits_memory;
DROP TABLE t_read_limits_memory_compressed;
DROP TABLE t_read_limits_memory_small_blocks;
DROP TABLE t_read_limits_memory_prewhere;
DROP TABLE t_read_limits_log;
DROP TABLE t_read_limits_tinylog;
DROP TABLE t_read_limits_stripelog;
