-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05896_one.parquet', Parquet) SELECT 7::Int32 AS A;
INSERT INTO FUNCTION file(currentDatabase() || '_05896_date.parquet', Parquet) SELECT toDate32('2200-01-01') AS D;
INSERT INTO FUNCTION file(currentDatabase() || '_05896_nested.parquet', Parquet)
SELECT tuple(7::Int32)::Tuple(A Int32) AS s, [tuple(7::Int32)]::Array(Tuple(A Int32)) AS arr, map('k', 7::Int32) AS M;
INSERT INTO FUNCTION file(currentDatabase() || '_05896_tuple_in_array.parquet', Parquet)
SELECT tuple(7::Int32, [tuple(8::Int32)])::Tuple(x Int32, arr Array(Tuple(q Int32))) AS s;
INSERT INTO FUNCTION file(currentDatabase() || '_05896_unsupported_plain.parquet', Parquet)
SELECT tuple(10::UInt8, 20::UInt8)::Tuple(bad_uint8 UInt8, good UInt8) AS S, 'ok' AS tail;
-- `S.bad_uint8`: INT32 -> DOUBLE in the footer, which does not fit its INTEGER(8, false) annotation (as in 05829).
INSERT INTO FUNCTION file(currentDatabase() || '_05896_unsupported.parquet', RawBLOB)
SELECT replaceOne(raw, unhex('150225001809') || 'bad_uint8', unhex('150A25001809') || 'bad_uint8')
FROM file(currentDatabase() || '_05896_unsupported_plain.parquet', RawBLOB, 'raw String');

SET input_format_parquet_case_insensitive_column_matching = 1;

-- Each requested column reads the field its name matches, with its own type, even if another one differs only by case.
SELECT * FROM file(currentDatabase() || '_05896_one.parquet', Parquet, 'a Int32, A Int32');
SELECT * FROM file(currentDatabase() || '_05896_one.parquet', Parquet, 'A Int32, a Int32');
SELECT * FROM file(currentDatabase() || '_05896_one.parquet', Parquet, 'a Int32, A Int32') SETTINGS input_format_parquet_allow_missing_columns = 0;
SELECT count() FROM file(currentDatabase() || '_05896_one.parquet', Parquet, 'a Int32, A Int32') WHERE a = 7 AND A = 7;
SELECT * FROM file(currentDatabase() || '_05896_date.parquet', Parquet, 'd Int32, D DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT * FROM file(currentDatabase() || '_05896_date.parquet', Parquet, 'D DateTime(\'UTC\'), d Int32') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT * FROM file(currentDatabase() || '_05896_nested.parquet', Parquet, '`s.a` Int32, `s.A` Int64');
SELECT * FROM file(currentDatabase() || '_05896_nested.parquet', Parquet, 's Tuple(A Int32), S Tuple(A Int64)');
SELECT * FROM file(currentDatabase() || '_05896_nested.parquet', Parquet, '`arr.a` Array(Int32), `arr.A` Array(Int64), `arr.missing` Array(Int32)');
SELECT * FROM file(currentDatabase() || '_05896_nested.parquet', Parquet, 'm Map(String, Int32), M Map(String, Int64)');
SELECT * FROM file(currentDatabase() || '_05896_tuple_in_array.parquet', Parquet, 's Tuple(x Int32), S Tuple(x Int64), `s.arr.missing` Array(Int32)');
SELECT * FROM file(currentDatabase() || '_05896_unsupported.parquet', Parquet, 's Tuple(good UInt8), S Tuple(bad_uint8 UInt8), tail String') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;
SELECT * FROM file(currentDatabase() || '_05896_unsupported.parquet', Parquet, 'S Tuple(bad_uint8 UInt8), s Tuple(good UInt8), tail String') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

SELECT * FROM file(currentDatabase() || '_05896_one.parquet', Parquet, 'a Int32, A Int32') SETTINGS input_format_parquet_case_insensitive_column_matching = 0;
SELECT * FROM file(currentDatabase() || '_05896_nested.parquet', Parquet, 's Tuple(A Int32), S Tuple(A Int64), `s.a` Int32'); -- { serverError COLUMN_QUERIED_MORE_THAN_ONCE }
