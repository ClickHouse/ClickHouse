-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not available in the fast test build.

-- A `Dynamic` value is written into the shredded `typed_value` of a `Parquet` `VARIANT` only when the
-- conversion to the selected leaf type is lossless. Values of another float width, decimal or
-- `DateTime64` scale, enum mapping or `FixedString` length stay in the residual `value`.

SET output_format_parquet_use_custom_encoder = 1, engine_file_truncate_on_insert = 1;
DROP TABLE IF EXISTS src;
CREATE TABLE src (k UInt8, d Dynamic) ENGINE = MergeTree ORDER BY k;
INSERT INTO src VALUES (1, toFloat32(1)), (2, toFloat32(2)), (3, toFloat64(16777217));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_f.parquet') SELECT d FROM src ORDER BY k;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '_f.parquet', Parquet, 'd Dynamic');
TRUNCATE TABLE src;
INSERT INTO src VALUES (1, toDecimal64('1.23', 2)), (2, toDecimal64('4.56', 2)), (3, toDecimal64('1.234', 3));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_f.parquet') SELECT d FROM src ORDER BY k;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '_f.parquet', Parquet, 'd Dynamic');
TRUNCATE TABLE src;
INSERT INTO src VALUES (1, toDateTime64('2020-01-01 00:00:00.12', 2, 'UTC')), (2, toDateTime64('2020-01-01 00:00:00.34', 2, 'UTC')), (3, toDateTime64('2020-01-01 00:00:00.567', 3, 'UTC'));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_f.parquet') SELECT d FROM src ORDER BY k;
SELECT d FROM file(currentDatabase() || '_f.parquet', Parquet, 'd Dynamic');
TRUNCATE TABLE src;
INSERT INTO src VALUES (1, CAST('a', 'Enum8(\'a\' = 1)')), (2, CAST('a', 'Enum8(\'a\' = 1)')), (3, CAST('b', 'Enum8(\'b\' = 1)'));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_f.parquet') SELECT d FROM src ORDER BY k SETTINGS output_format_parquet_enum_as_byte_array = 1;
SELECT d FROM file(currentDatabase() || '_f.parquet', Parquet, 'd Dynamic');
TRUNCATE TABLE src;
INSERT INTO src VALUES (1, toFixedString('abc', 3)), (2, toFixedString('def', 3)), (3, 'g');
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_f.parquet') SELECT d FROM src ORDER BY k;
SELECT hex(toString(d)) FROM file(currentDatabase() || '_f.parquet', Parquet, 'd Dynamic');
DROP TABLE src;
