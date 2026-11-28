-- Tags: no-fasttest
-- no-fasttest: the Avro format is not built in the fast test

-- A null, a variant null, an empty object and a null field of a variant read as `JSON` store the default of each typed path.
INSERT INTO FUNCTION file(currentDatabase() || '_variant_null_enum.avro') SELECT * FROM values('id UInt8, v Nullable(Tuple(metadata String, value String))', (1, NULL), (2, (unhex('010000'), unhex('00'))), (3, (unhex('010000'), unhex('020000'))), (4, (unhex('0101000161'), unhex('020100000100')))) SETTINGS engine_file_truncate_on_insert = 1, output_format_avro_string_column_pattern = '^never$';
INSERT INTO FUNCTION file(currentDatabase() || '_variant_null_enum.parquet') SELECT * FROM values('id UInt8, v Nullable(Tuple(metadata String, value String))', (1, NULL), (2, (unhex('010000'), unhex('00'))), (3, (unhex('010000'), unhex('020000'))), (4, (unhex('0101000161'), unhex('020100000100')))) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_string_as_string = 0;

SELECT 'Avro', id, v.a, v.b, v.t FROM file(currentDatabase() || '_variant_null_enum.avro', Avro, 'id UInt8, v JSON(a Enum8(\'x\' = 5), b Enum8(\'m\' = -3, \'n\' = 0), t Tuple(e Enum8(\'y\' = 3)))') ORDER BY id;
SELECT 'Parquet', id, v.a, v.b, v.t FROM file(currentDatabase() || '_variant_null_enum.parquet', Parquet, 'id UInt8, v JSON(a Enum8(\'x\' = 5), b Enum8(\'m\' = -3, \'n\' = 0), t Tuple(e Enum8(\'y\' = 3)))') ORDER BY id;
SELECT 'Nullable', id, isNull(v), v.a FROM file(currentDatabase() || '_variant_null_enum.parquet', Parquet, 'id UInt8, v Nullable(JSON(a Enum8(\'x\' = 5)))') ORDER BY id;
