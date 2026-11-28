-- `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` applies to schema inference only: reading a
-- column of a type the reader cannot read fails as without the setting.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05830_int64.parquet')
SELECT number::Int64 AS w, number AS x FROM numbers(3);
-- DECIMAL(77, 0), wider than any ClickHouse `Decimal`.
INSERT INTO FUNCTION file(currentDatabase() || '_05830_wide.parquet')
SELECT (number * 1000)::Int256 AS w, number + 10 AS x FROM numbers(3)
SETTINGS output_format_parquet_wide_integer_as_decimal = 1;

SET input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

DESCRIBE file(currentDatabase() || '_05830_wide.parquet');
SELECT * FROM file(currentDatabase() || '_05830_wide.parquet') ORDER BY x;
SELECT w, x FROM file(currentDatabase() || '_05830_wide.parquet', Parquet, 'w Int256, x UInt64') ORDER BY x;
SELECT w, x FROM file(currentDatabase() || '_05830_wide.parquet', Parquet, 'w Int64, x UInt64'); -- { serverError INCORRECT_DATA }
SELECT sum(w) FROM file(currentDatabase() || '_05830_{int64,wide}.parquet') SETTINGS schema_inference_mode = 'union'; -- { serverError INCORRECT_DATA }

-- An array whose requested elements are not in the file takes its sizes from a leaf under it. The only leaf here gets a
-- FIXED_LEN_BYTE_ARRAY length of 0 in the footer (compact protocol: `type_length` 3 -> 0), so no leaf can be read.
INSERT INTO FUNCTION file(currentDatabase() || '_05830_arr.parquet')
SELECT [tuple(toFixedString('abc', 3)), tuple(toFixedString('def', 3))]::Array(Tuple(a FixedString(3))) AS arr, number AS x
FROM numbers(3)
SETTINGS output_format_parquet_fixed_string_as_fixed_byte_array = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05830_arr_len0.parquet', RawBLOB)
SELECT replaceOne(raw, unhex('150E15061500180161'), unhex('150E15001500180161'))
FROM file(currentDatabase() || '_05830_arr.parquet', RawBLOB, 'raw String');
SELECT countSubstrings(raw, unhex('150E15001500180161')) FROM file(currentDatabase() || '_05830_arr_len0.parquet', RawBLOB, 'raw String');
SELECT arr, x FROM file(currentDatabase() || '_05830_arr_len0.parquet', Parquet, 'arr Array(Tuple(b Int32)), x UInt64'); -- { serverError INCORRECT_DATA }
