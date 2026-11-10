-- Tags: no-fasttest
-- no-fasttest: depends on s3 storage

-- An out-of-range physical type, repetition type or converted type of a Parquet schema element is
-- rejected when the footer is read, whichever columns the query reads.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05402_plain.parquet', format = Parquet)
SELECT 1::Int32 AS bad_repetition_type, 1::Int32 AS bad_physical_type, 'a' AS bad_converted_type, 1::Int32 AS x;

INSERT INTO FUNCTION s3(s3_conn, filename = '05402_file.parquet', format = Parquet)
SELECT CAST(tuple('a'), 'Tuple(uri Nullable(String))') AS f
SETTINGS output_format_parquet_write_file_logical_type = 1;

-- Each file patches one value of a schema element in the footer (compact protocol: field header byte, zigzag value).
-- repetition_type REQUIRED -> 4
INSERT INTO FUNCTION s3(s3_conn, filename = '05402_repetition_type.parquet', format = RawBLOB)
SELECT replaceOne(raw, unhex('25001813') || 'bad_repetition_type', unhex('25081813') || 'bad_repetition_type')
FROM s3(s3_conn, filename = '05402_plain.parquet', format = RawBLOB, structure = 'raw String');
-- type INT32 -> -7
INSERT INTO FUNCTION s3(s3_conn, filename = '05402_physical_type.parquet', format = RawBLOB)
SELECT replaceOne(raw, unhex('150225001811') || 'bad_physical_type', unhex('150D25001811') || 'bad_physical_type')
FROM s3(s3_conn, filename = '05402_plain.parquet', format = RawBLOB, structure = 'raw String');
-- converted_type UTF8 -> 40
INSERT INTO FUNCTION s3(s3_conn, filename = '05402_converted_type.parquet', format = RawBLOB)
SELECT replaceOne(raw, 'bad_converted_type' || unhex('2500'), 'bad_converted_type' || unhex('2550'))
FROM s3(s3_conn, filename = '05402_plain.parquet', format = RawBLOB, structure = 'raw String');
-- type BYTE_ARRAY -> -7 for the field `uri` of a `FILE` group
INSERT INTO FUNCTION s3(s3_conn, filename = '05402_file_field_type.parquet', format = RawBLOB)
SELECT replaceOne(raw, unhex('150C25021803') || 'uri', unhex('150D25021803') || 'uri')
FROM s3(s3_conn, filename = '05402_file.parquet', format = RawBLOB, structure = 'raw String');

-- Every patch landed.
SELECT
    (SELECT countSubstrings(raw, unhex('25081813') || 'bad_repetition_type') FROM s3(s3_conn, filename = '05402_repetition_type.parquet', format = RawBLOB, structure = 'raw String')),
    (SELECT countSubstrings(raw, unhex('150D25001811') || 'bad_physical_type') FROM s3(s3_conn, filename = '05402_physical_type.parquet', format = RawBLOB, structure = 'raw String')),
    (SELECT countSubstrings(raw, 'bad_converted_type' || unhex('2550')) FROM s3(s3_conn, filename = '05402_converted_type.parquet', format = RawBLOB, structure = 'raw String')),
    (SELECT countSubstrings(raw, unhex('150D25021803') || 'uri') FROM s3(s3_conn, filename = '05402_file_field_type.parquet', format = RawBLOB, structure = 'raw String'));

-- `x` is valid in every file: the invalid element is rejected although the query does not read it.
SELECT x FROM s3(s3_conn, filename = '05402_repetition_type.parquet', format = Parquet, structure = 'x Int32') SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT x FROM s3(s3_conn, filename = '05402_physical_type.parquet', format = Parquet, structure = 'x Int32') SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT x FROM s3(s3_conn, filename = '05402_converted_type.parquet', format = Parquet, structure = 'x Int32') SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT count() FROM s3(s3_conn, filename = '05402_converted_type.parquet', format = Parquet, structure = 'x Int32') SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT f FROM s3(s3_conn, filename = '05402_file_field_type.parquet', format = Parquet, structure = 'f Tuple(uri Nullable(String))') SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
DESCRIBE s3(s3_conn, filename = '05402_converted_type.parquet', format = Parquet); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT * FROM s3(s3_conn, filename = '05402_plain.parquet', format = Parquet) SETTINGS use_parquet_metadata_cache = 1;
SELECT * FROM s3(s3_conn, filename = '05402_file.parquet', format = Parquet) SETTINGS use_parquet_metadata_cache = 1;
