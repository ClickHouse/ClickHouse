-- Tags: no-fasttest
-- no-fasttest: requires S3 and MinIO.

INSERT INTO FUNCTION s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64')
SELECT number FROM numbers(100000) SETTINGS s3_truncate_on_insert = 1;

SET max_threads = 1, max_parsing_threads = 1, max_block_size = 1000, input_format_parallel_parsing = 0;

SELECT sum(n) FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64');

SELECT sum(n) FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(n) FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }
SELECT sum(n) FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(n) FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT sum(n) < 4999950000 FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_rows_to_read = 1000, read_overflow_mode = 'break';
SELECT sum(n) < 4999950000 FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_bytes_to_read = 1000, read_overflow_mode = 'break';
SELECT sum(n) < 4999950000 FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';
SELECT sum(n) < 4999950000 FROM s3(s3_conn, filename = concat(currentDatabase(), '_05398_read_limits.csv'), format = CSV, structure = 'n UInt64') SETTINGS max_bytes_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';

-- The same object served over HTTP exercises the separate URL source.
DROP TABLE IF EXISTS t_url_read_limits;
CREATE TABLE t_url_read_limits (n UInt64)
ENGINE = URL(concat('http://localhost:11111/test/', currentDatabase(), '_05398_read_limits.csv'), CSV);

SELECT sum(n) FROM t_url_read_limits;
SELECT sum(n) FROM t_url_read_limits SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(n) FROM t_url_read_limits SETTINGS max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }
SELECT sum(n) FROM t_url_read_limits SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }
SELECT sum(n) FROM t_url_read_limits SETTINGS max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT sum(n) < 4999950000 FROM t_url_read_limits SETTINGS max_rows_to_read = 1000, read_overflow_mode = 'break';
SELECT sum(n) < 4999950000 FROM t_url_read_limits SETTINGS max_bytes_to_read = 1000, read_overflow_mode = 'break';
SELECT sum(n) < 4999950000 FROM t_url_read_limits SETTINGS max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';
SELECT sum(n) < 4999950000 FROM t_url_read_limits SETTINGS max_bytes_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';

DROP TABLE t_url_read_limits;
