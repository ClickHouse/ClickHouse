-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- Markers of missing columns are kept in data parts in Parquet format,
-- and the columns with the `Quantized` codec are not stored in Parquet format.

DROP TABLE IF EXISTS t_parquet_missing;

CREATE TABLE t_parquet_missing (k UInt64, b UInt64, payload String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_parquet_missing VALUES (1, 0, 'first');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_missing' AND active;
ALTER TABLE t_parquet_missing DETACH PARTITION tuple();
ALTER TABLE t_parquet_missing MODIFY COLUMN b String;
ALTER TABLE t_parquet_missing ATTACH PARTITION tuple();
INSERT INTO t_parquet_missing VALUES (2, '', 'second');
SELECT k, b, length(b) FROM t_parquet_missing ORDER BY k;
OPTIMIZE TABLE t_parquet_missing FINAL;
SELECT k, b, length(b) FROM t_parquet_missing ORDER BY k;

DROP TABLE t_parquet_missing;

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_parquet_quantize;

CREATE TABLE t_parquet_quantize (id UInt64, vec Array(Float32) CODEC(Quantized('product', 8, 4, 2)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1;

INSERT INTO t_parquet_quantize SELECT number, [number, number + 1, number + 2, number + 3, number + 4, number + 5, number + 6, number + 7] FROM numbers(300);
SELECT part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_quantize' AND active;
SELECT count(), uniqExact(length(vec.quantized)) FROM t_parquet_quantize;

DROP TABLE t_parquet_quantize;
