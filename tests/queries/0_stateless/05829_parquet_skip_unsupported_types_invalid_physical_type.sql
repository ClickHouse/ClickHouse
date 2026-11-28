-- `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` on a column with an invalid physical type.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05829_plain.parquet')
SELECT number::Int32 AS x, toFixedString(toString(10000 + number), 5) AS invalid, (10 + number)::UInt8 AS bad_uint8, 's' || toString(number) AS s
FROM numbers(3);

-- Each file patches the physical type of one schema element in the footer (compact protocol: field header, zigzag value).
-- `invalid`: FIXED_LEN_BYTE_ARRAY (7) -> 42, out of the enum.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_garbage.parquet', RawBLOB)
SELECT replaceOne(raw, unhex('150E150A15001807') || 'invalid', unhex('1554150A15001807') || 'invalid')
FROM file(currentDatabase() || '_05829_plain.parquet', RawBLOB, 'raw String');
-- `bad_uint8`: INT32 (1) -> DOUBLE (5), a valid type that does not fit its INTEGER(8, false) annotation.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_mismatch.parquet', RawBLOB)
SELECT replaceOne(raw, unhex('150225001809') || 'bad_uint8', unhex('150A25001809') || 'bad_uint8')
FROM file(currentDatabase() || '_05829_plain.parquet', RawBLOB, 'raw String');

-- Both patches landed.
SELECT
    (SELECT (countSubstrings(raw, unhex('150E150A15001807') || 'invalid'), countSubstrings(raw, unhex('1554150A15001807') || 'invalid')) FROM file(currentDatabase() || '_05829_garbage.parquet', RawBLOB, 'raw String')),
    (SELECT (countSubstrings(raw, unhex('150225001809') || 'bad_uint8'), countSubstrings(raw, unhex('150A25001809') || 'bad_uint8')) FROM file(currentDatabase() || '_05829_mismatch.parquet', RawBLOB, 'raw String'));

-- A physical type out of the enum makes the file invalid, so the setting does not skip the column.
DESCRIBE file(currentDatabase() || '_05829_garbage.parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESCRIBE file(currentDatabase() || '_05829_garbage.parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT x FROM file(currentDatabase() || '_05829_garbage.parquet', Parquet, 'x Int32') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1; -- { serverError INCORRECT_DATA }

-- A valid physical type that does not fit the column's type is skipped, and the other columns are read.
DESCRIBE file(currentDatabase() || '_05829_mismatch.parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;
SELECT * FROM file(currentDatabase() || '_05829_mismatch.parquet') ORDER BY x SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;
SELECT s FROM file(currentDatabase() || '_05829_mismatch.parquet') WHERE x = 1 SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;
DESCRIBE file(currentDatabase() || '_05829_mismatch.parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
-- An explicit structure without the column reads the file whatever the setting is; requesting the column fails.
SELECT x, s FROM file(currentDatabase() || '_05829_mismatch.parquet', Parquet, 'x Int32, s String') ORDER BY x SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0;
SELECT bad_uint8 FROM file(currentDatabase() || '_05829_mismatch.parquet', Parquet, 'bad_uint8 UInt8') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0; -- { serverError INCORRECT_DATA }
