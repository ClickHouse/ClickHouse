-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- Data parts are not created in Parquet format when an encryption codec applies to some column, explicitly or by default.
-- Values of DateTime64 with scales 7 and 8 out of the range of Parquet timestamps are read back without changes.
-- The sizes of the columns of Parquet parts are known.

DROP TABLE IF EXISTS t_parquet_encrypted;
DROP TABLE IF EXISTS t_parquet_encrypted_default;
DROP TABLE IF EXISTS t_parquet_datetime64;
DROP TABLE IF EXISTS t_parquet_sizes;

CREATE TABLE t_parquet_encrypted (k UInt64, s String CODEC(AES_128_GCM_SIV))
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_encrypted VALUES (1, 'a');
SELECT 'explicit encryption codec', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_encrypted' AND active;

CREATE TABLE t_parquet_encrypted_default (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'AES_128_GCM_SIV';
INSERT INTO t_parquet_encrypted_default VALUES (1, 'a');
SELECT 'default encryption codec', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_encrypted_default' AND active;
SELECT * FROM t_parquet_encrypted_default;

CREATE TABLE t_parquet_datetime64 (k UInt64, d7 DateTime64(7, 'UTC'), d8 DateTime64(8, 'UTC'), d9 DateTime64(9, 'UTC'))
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_datetime64 VALUES (1, '2290-01-01 00:00:00.1234567', '2299-12-31 23:59:59.12345678', '2262-04-11 23:47:16'), (2, '1900-01-01 00:00:00', '1900-01-01 00:00:00', '1900-01-01 00:00:00');
SELECT 'datetime64', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_datetime64' AND active;
SELECT * FROM t_parquet_datetime64 ORDER BY k;

CREATE TABLE t_parquet_sizes (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1000;
INSERT INTO t_parquet_sizes SELECT number, repeat('x', 100) FROM numbers(10000);
SELECT 'sizes';
SELECT name, data_compressed_bytes > 0, data_compressed_bytes < 100000, data_uncompressed_bytes > 1000000
FROM system.columns WHERE database = currentDatabase() AND table = 't_parquet_sizes' ORDER BY name;
SELECT part_type, data_uncompressed_bytes > 1000000 FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_sizes' AND active;
CHECK TABLE t_parquet_sizes SETTINGS check_query_single_value_result = 1;

DROP TABLE t_parquet_encrypted;
DROP TABLE t_parquet_encrypted_default;
DROP TABLE t_parquet_datetime64;
DROP TABLE t_parquet_sizes;
