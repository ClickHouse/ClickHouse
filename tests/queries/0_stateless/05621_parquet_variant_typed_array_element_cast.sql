-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not available in the fast test build.

-- A shredded `Parquet` `VARIANT` array with `INT64` elements read with a narrower requested element
-- type: the elements must be converted to the requested type, not reinterpreted.

SET engine_file_truncate_on_insert = 1, enable_json_type = 1;

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_typed_array.parquet')
SELECT '{"a":[1,2]}'::JSON AS j SETTINGS output_format_parquet_json_as_variant = 1;

SELECT `j.a` FROM file(currentDatabase() || '_typed_array.parquet', Parquet, '`j.a` Array(UInt8)');
SELECT j.a FROM file(currentDatabase() || '_typed_array.parquet', Parquet, 'j JSON(a Array(UInt8))');
SELECT `j.a` FROM file(currentDatabase() || '_typed_array.parquet', Parquet, '`j.a` Array(Int16)');
SELECT `j.a` FROM file(currentDatabase() || '_typed_array.parquet', Parquet, '`j.a` Array(String)');
