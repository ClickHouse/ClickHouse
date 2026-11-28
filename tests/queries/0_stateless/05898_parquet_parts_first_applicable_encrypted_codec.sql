-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL and Parquet

-- `FirstApplicable` selects a codec for the data type of every substream, so an alternative with an encryption
-- codec may apply only to some of the data types. The codec resolved without a data type is then not an encryption
-- codec, but the data must not be written unencrypted into a part in `Parquet` format: such a table never uses it.

SET allow_experimental_parquet_parts = 1;
SET enable_subcolumn_codecs = 1;

DROP TABLE IF EXISTS t_parquet_fa_tuple;
DROP TABLE IF EXISTS t_parquet_fa_primitive;
DROP TABLE IF EXISTS t_parquet_fa_default;
DROP TABLE IF EXISTS t_parquet_fa_default_in_column;
DROP TABLE IF EXISTS t_parquet_fa_subcolumn;
DROP TABLE IF EXISTS t_parquet_fa_nested;
DROP TABLE IF EXISTS t_parquet_fa_recompress;
DROP TABLE IF EXISTS t_parquet_fa_projection;
DROP TABLE IF EXISTS t_parquet_fa_not_encrypted;

-- The codec of a `Tuple` is resolved for the types of its elements: `Delta, LZ4, AES_128_GCM_SIV` for `UInt64`.
CREATE TABLE t_parquet_fa_tuple (p Tuple(a UInt64) CODEC(FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_fa_tuple SELECT tuple(toUInt64(42));
SELECT 'tuple', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_tuple' AND active;
SELECT * FROM t_parquet_fa_tuple;

CREATE TABLE t_parquet_fa_primitive (k UInt64 CODEC(FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4)), s String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_fa_primitive VALUES (1, 'a');
SELECT 'primitive', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_primitive' AND active;
SELECT * FROM t_parquet_fa_primitive;

-- The same for the default codec of the table.
CREATE TABLE t_parquet_fa_default (k UInt64, p Tuple(a UInt64), s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4)';
INSERT INTO t_parquet_fa_default VALUES (1, (2), 'a');
SELECT 'default', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_default' AND active;
SELECT * FROM t_parquet_fa_default;

-- `Default` in the codec of a column stands for the default codec of the table.
CREATE TABLE t_parquet_fa_default_in_column (k UInt64, p Tuple(a UInt64) CODEC(FirstApplicable(Multiple(Delta, Default), Default)))
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'FirstApplicable(Multiple(T64, AES_128_GCM_SIV), ZSTD)';
INSERT INTO t_parquet_fa_default_in_column VALUES (1, (2));
SELECT 'default in column', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_default_in_column' AND active;
SELECT * FROM t_parquet_fa_default_in_column;

-- The codecs of subcolumns.
CREATE TABLE t_parquet_fa_subcolumn
(
    k UInt64,
    p Tuple(a UInt64, b String),
    CODEC p.a (FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4))
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_fa_subcolumn VALUES (1, (2, 'a'));
SELECT 'subcolumn', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_subcolumn' AND active;
SELECT * FROM t_parquet_fa_subcolumn;

-- An alternative that is a nested `FirstApplicable`.
CREATE TABLE t_parquet_fa_nested (k UInt64, p Tuple(a Float64) CODEC(FirstApplicable(Multiple(T64, ZSTD), FirstApplicable(Multiple(Gorilla, AES_256_GCM_SIV), ZSTD))))
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_fa_nested VALUES (1, (2));
SELECT 'nested', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_nested' AND active;
SELECT * FROM t_parquet_fa_nested;

-- The codec of a `RECOMPRESS` TTL rule.
CREATE TABLE t_parquet_fa_recompress (k UInt64, d Date, p Tuple(a UInt64))
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 100 YEAR RECOMPRESS CODEC(FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4))
SETTINGS min_rows_for_parquet_part = 1;
INSERT INTO t_parquet_fa_recompress VALUES (1, '2020-01-01', (2));
SELECT 'recompress', part_type != 'Parquet' FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_recompress' AND active;
SELECT * FROM t_parquet_fa_recompress;

-- A projection part inherits the codec of its parent part.
CREATE TABLE t_parquet_fa_projection (k UInt64, p Tuple(a UInt64), PROJECTION pr (SELECT * ORDER BY p) WITH SETTINGS (min_rows_for_parquet_part = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'FirstApplicable(Multiple(Delta, LZ4, AES_128_GCM_SIV), LZ4)';
INSERT INTO t_parquet_fa_projection VALUES (1, (2));
SELECT 'projection', part_type != 'Parquet' FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_parquet_fa_projection' AND active;
SELECT * FROM t_parquet_fa_projection;

-- `FirstApplicable` without encryption codecs does not prevent the format.
CREATE TABLE t_parquet_fa_not_encrypted (k UInt64 CODEC(FirstApplicable(Multiple(Delta, LZ4), ZSTD)), p Tuple(a UInt64) CODEC(FirstApplicable(Multiple(Delta, LZ4), ZSTD)), s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'FirstApplicable(Multiple(Delta, ZSTD), LZ4)';
INSERT INTO t_parquet_fa_not_encrypted VALUES (1, (2), 'a');
SELECT 'not encrypted', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_fa_not_encrypted' AND active;
SELECT * FROM t_parquet_fa_not_encrypted;

DROP TABLE t_parquet_fa_tuple;
DROP TABLE t_parquet_fa_primitive;
DROP TABLE t_parquet_fa_default;
DROP TABLE t_parquet_fa_default_in_column;
DROP TABLE t_parquet_fa_subcolumn;
DROP TABLE t_parquet_fa_nested;
DROP TABLE t_parquet_fa_recompress;
DROP TABLE t_parquet_fa_projection;
DROP TABLE t_parquet_fa_not_encrypted;
