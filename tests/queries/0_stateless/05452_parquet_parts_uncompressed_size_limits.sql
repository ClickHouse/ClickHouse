-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The uncompressed size of a part in `Parquet` format includes the uncompressed sizes of its columns,
-- so the limits on the uncompressed size of the table and of the patch parts apply to such parts.

DROP TABLE IF EXISTS t_parquet_size_limit;
DROP TABLE IF EXISTS t_parquet_patches_limit;

CREATE TABLE t_parquet_size_limit (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, max_table_size_bytes_uncompressed = 65536;

INSERT INTO t_parquet_size_limit SELECT number, repeat('x', 1024) FROM numbers(1000) SETTINGS max_insert_threads = 1;
INSERT INTO t_parquet_size_limit SELECT number + 1000, repeat('y', 1024) FROM numbers(1000) SETTINGS max_insert_threads = 1; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

SELECT part_type, data_uncompressed_bytes > 1000000 FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_size_limit' AND active;
SELECT total_bytes_uncompressed > 1000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_size_limit';

DETACH TABLE t_parquet_size_limit;
ATTACH TABLE t_parquet_size_limit;

SELECT total_bytes_uncompressed > 1000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_size_limit';
INSERT INTO t_parquet_size_limit SELECT number + 1000, repeat('y', 1024) FROM numbers(1000) SETTINGS max_insert_threads = 1; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_parquet_size_limit;

CREATE TABLE t_parquet_patches_limit (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, enable_block_number_column = 1, enable_block_offset_column = 1, max_uncompressed_bytes_in_patches = 1500000;

SET enable_lightweight_update = 1;

SYSTEM STOP MERGES t_parquet_patches_limit;
INSERT INTO t_parquet_patches_limit SELECT number, '' FROM numbers(1000) SETTINGS max_insert_threads = 1;

UPDATE t_parquet_patches_limit SET s = repeat('x', 1024) WHERE 1;
UPDATE t_parquet_patches_limit SET s = repeat('y', 1024) WHERE 1; -- { serverError TOO_LARGE_LIGHTWEIGHT_UPDATES }

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_patches_limit' AND active AND startsWith(name, 'patch-');
SELECT count() FROM t_parquet_patches_limit WHERE s = repeat('x', 1024);

DROP TABLE t_parquet_size_limit;
DROP TABLE t_parquet_patches_limit;
