-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The numeric types without a lossless representation in Parquet, e.g. `BFloat16` and `DateTime64(7)`, are stored in
-- the ClickHouse binary format. Their uncompressed size must be the size of the values, without the offsets of the
-- binary strings: a debug build checks the number of rows of a loaded part by the size of a numeric column.

SET allow_experimental_parquet_parts = 1;

DROP TABLE IF EXISTS t_parquet_binary_numeric;

CREATE TABLE t_parquet_binary_numeric (id UInt64, bf16 BFloat16, dt64_7 DateTime64(7, 'UTC'))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1;

INSERT INTO t_parquet_binary_numeric VALUES (1, 1.5, '2026-11-09 01:02:03.1234567');
INSERT INTO t_parquet_binary_numeric SELECT number + 2, number, toDateTime64('2026-11-09 00:00:00', 7, 'UTC') + number FROM numbers(1000);

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_binary_numeric' AND active ORDER BY name;
SELECT column, rows, column_data_uncompressed_bytes FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parquet_binary_numeric' AND active AND column != 'id' ORDER BY name, column;

DETACH TABLE t_parquet_binary_numeric;
ATTACH TABLE t_parquet_binary_numeric;

SELECT count(), sum(bf16), max(dt64_7) FROM t_parquet_binary_numeric;
SELECT * FROM t_parquet_binary_numeric WHERE id = 1;

DROP TABLE t_parquet_binary_numeric;
