-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;

SELECT '-- An empty root key is written and read back';
INSERT INTO FUNCTION file(currentDatabase() || '_05507_empty_key.parquet', Parquet)
SELECT CAST('{"":1}' AS JSON) AS j;
SELECT toJSONString(j) FROM file(currentDatabase() || '_05507_empty_key.parquet', Parquet, 'j JSON');

SELECT '-- Keys of a JSON object inside VARIANT are not a FILE group';
INSERT INTO FUNCTION file(currentDatabase() || '_05507_file.parquet', Parquet)
SELECT CAST('{"size":1}', 'JSON(max_dynamic_paths=0, size UInt64)') AS j
SETTINGS output_format_parquet_write_file_logical_type = 1;
SELECT toJSONString(j) FROM file(currentDatabase() || '_05507_file.parquet', Parquet, 'j JSON');

SELECT '-- A nested object read as String has no trailing padding';
INSERT INTO FUNCTION file(currentDatabase() || '_05507_nested_string.parquet', Parquet)
SELECT CAST('{"a":[{"b":1}]}', 'JSON(max_dynamic_paths=0)') AS j;
SELECT j.a, length(j.a[1]) FROM file(currentDatabase() || '_05507_nested_string.parquet', Parquet, 'j JSON(a Array(String))');

SELECT '-- A shredded array path whose residual mixes a nested array with null';
INSERT INTO FUNCTION file(currentDatabase() || '_05507_array_null.parquet', Parquet)
SELECT CAST(s, 'JSON') AS j FROM (SELECT arrayJoin(['{"a":[[1],null]}', '{"a":[2,3,4]}']) AS s);
SELECT toJSONString(j) FROM file(currentDatabase() || '_05507_array_null.parquet', Parquet, 'j JSON') ORDER BY toString(j);

SELECT '-- A value that does not fit the selected integer leaf stays in the residual';
INSERT INTO FUNCTION file(currentDatabase() || '_05507_int_width.parquet', Parquet)
SELECT arrayJoin([
    CAST(1::UInt8 AS Dynamic), CAST(2::UInt8 AS Dynamic), CAST(256::UInt64 AS Dynamic),
    CAST(-1::Int8 AS Dynamic), CAST(-2::Int8 AS Dynamic), CAST(-1000::Int64 AS Dynamic)]) AS d;
SELECT d FROM file(currentDatabase() || '_05507_int_width.parquet', Parquet, 'd Dynamic');
