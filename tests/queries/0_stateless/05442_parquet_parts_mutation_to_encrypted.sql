-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL and Parquet

-- A mutation of a data part in Parquet format writes a part in another format if an encryption codec
-- applies to the table since the part was written.

DROP TABLE IF EXISTS t_parquet_mutation_default_codec;
DROP TABLE IF EXISTS t_parquet_mutation_new_column;

CREATE TABLE t_parquet_mutation_default_codec (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_mutation_default_codec VALUES (1, 'a');
SELECT 'before', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutation_default_codec' AND active;

ALTER TABLE t_parquet_mutation_default_codec MODIFY SETTING default_compression_codec = 'AES_128_GCM_SIV';
ALTER TABLE t_parquet_mutation_default_codec UPDATE s = 'b' WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT 'default codec', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutation_default_codec' AND active;
SELECT * FROM t_parquet_mutation_default_codec;

CREATE TABLE t_parquet_mutation_new_column (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'LZ4';
INSERT INTO t_parquet_mutation_new_column VALUES (1, 'a');
SELECT 'before', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutation_new_column' AND active;

ALTER TABLE t_parquet_mutation_new_column ADD COLUMN e String DEFAULT 'x' CODEC(AES_128_GCM_SIV);
ALTER TABLE t_parquet_mutation_new_column MATERIALIZE COLUMN e SETTINGS mutations_sync = 2;
SELECT 'new column', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_mutation_new_column' AND active;
SELECT * FROM t_parquet_mutation_new_column;

DROP TABLE t_parquet_mutation_default_codec;
DROP TABLE t_parquet_mutation_new_column;
