-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The uncompressed size of a column in a part in `Parquet` format is the size of the full values, even when the
-- column is written from a sparse column, as a column read from a `Wide` part with sparse serialization is,
-- so the limit on the uncompressed size of the table applies to it.

SET allow_experimental_parquet_parts = 1;

DROP TABLE IF EXISTS t_parquet_sparse_size_src;
DROP TABLE IF EXISTS t_parquet_sparse_size_dst;

CREATE TABLE t_parquet_sparse_size_src (id UInt64, s UInt256)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_parquet_sparse_size_src SELECT number, toUInt256(number % 1000 = 0) FROM numbers(100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parquet_sparse_size_src' AND active ORDER BY column;

CREATE TABLE t_parquet_sparse_size_dst (id UInt64, s UInt256)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, max_table_size_bytes_uncompressed = 65536;

INSERT INTO t_parquet_sparse_size_dst SELECT * FROM t_parquet_sparse_size_src SETTINGS max_insert_threads = 1, max_threads = 1;

SELECT part_type, data_uncompressed_bytes >= 4000000 FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_sparse_size_dst' AND active;
SELECT name, data_uncompressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_parquet_sparse_size_dst' ORDER BY name;
SELECT total_bytes_uncompressed >= 4000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_sparse_size_dst';

INSERT INTO t_parquet_sparse_size_dst SELECT * FROM t_parquet_sparse_size_src SETTINGS max_insert_threads = 1, max_threads = 1; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

DETACH TABLE t_parquet_sparse_size_dst;
ATTACH TABLE t_parquet_sparse_size_dst;

SELECT total_bytes_uncompressed >= 4000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_sparse_size_dst';
SELECT count(), sum(s) FROM t_parquet_sparse_size_dst;

DROP TABLE t_parquet_sparse_size_src;
DROP TABLE t_parquet_sparse_size_dst;
