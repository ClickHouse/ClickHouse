-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The uncompressed size of a `LowCardinality` column in a part in `Parquet` format counts the values that the rows
-- refer to, even when the column shares the dictionary, as a column read from a `Wide` part does, so the limit on
-- the uncompressed size of the table applies to it.

SET allow_experimental_parquet_parts = 1;

DROP TABLE IF EXISTS t_parquet_lc_size_src;
DROP TABLE IF EXISTS t_parquet_lc_size_dst;

CREATE TABLE t_parquet_lc_size_src (id UInt64, s LowCardinality(String), a Array(LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_parquet_lc_size_src
SELECT number, concat(toString(number), repeat('x', 1024)), [concat(toString(number), repeat('y', 1024))]
FROM numbers(1000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_lc_size_src' AND active;

CREATE TABLE t_parquet_lc_size_dst (id UInt64, s LowCardinality(String), a Array(LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, max_table_size_bytes_uncompressed = 65536;

INSERT INTO t_parquet_lc_size_dst SELECT * FROM t_parquet_lc_size_src SETTINGS max_insert_threads = 1, max_threads = 1;

SELECT part_type, data_uncompressed_bytes > 2000000 FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_lc_size_dst' AND active;
SELECT name, data_uncompressed_bytes > 1000000 FROM system.columns WHERE database = currentDatabase() AND table = 't_parquet_lc_size_dst' ORDER BY name;
SELECT total_bytes_uncompressed > 2000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_lc_size_dst';

INSERT INTO t_parquet_lc_size_dst SELECT * FROM t_parquet_lc_size_src SETTINGS max_insert_threads = 1, max_threads = 1; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

DETACH TABLE t_parquet_lc_size_dst;
ATTACH TABLE t_parquet_lc_size_dst;

SELECT total_bytes_uncompressed > 2000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_lc_size_dst';
SELECT count(), sum(length(s)), sum(length(a[1])) FROM t_parquet_lc_size_dst;

DROP TABLE t_parquet_lc_size_src;
DROP TABLE t_parquet_lc_size_dst;
