-- `JSONExtract` and a `JSON` typed path must not round an integer just below the minimum of a signed type to the minimum.

SELECT JSONExtract('"-9223372036854775809"', 'Nullable(Int64)');
SELECT JSONExtract('-9223372036854775809', 'Nullable(Int64)');
SELECT JSONExtract('{"a":-9223372036854775809}', 'a', 'Nullable(Int64)');
SELECT JSONExtract('"-9223372036854776808"', 'Nullable(Int64)');
SELECT JSONExtract('"-09223372036854775809"', 'Nullable(Int64)');
SELECT JSONExtract('"-9223372036854775809"', 'LowCardinality(Nullable(Int64))');
SELECT JSONExtract('["-9223372036854775809", "-9223372036854775808"]', 'Array(Nullable(Int64))');
SELECT JSONExtractKeysAndValues('{"a":"-9223372036854775809","b":-9223372036854775809}', 'Nullable(Int64)');
SELECT JSONExtractInt('"-9223372036854775809"');
SELECT variantType(JSONExtract('"-9223372036854775809"', 'Variant(Int64, String)'));
SELECT CAST('{"a":"-9223372036854775809"}' AS JSON(a Int64)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":-9223372036854775809}' AS JSON(a Int64)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"-9223372036854775809"}' AS JSON(a Nullable(Int64))).a SETTINGS type_json_skip_invalid_typed_paths = 1;
SELECT * FROM format(JSONEachRow, 'j JSON(a Int64)', '{"j":{"a":"-9223372036854775809"}}'); -- { serverError INCORRECT_DATA }

-- Values that already behaved correctly.
SELECT JSONExtract('"-9223372036854775808"', 'Nullable(Int64)');
SELECT JSONExtract('-9223372036854775808', 'Nullable(Int64)');
SELECT JSONExtract('"9223372036854775807"', 'Nullable(Int64)');
SELECT JSONExtract('"9223372036854775808"', 'Nullable(Int64)');
SELECT JSONExtract('"-2147483649"', 'Nullable(Int32)');
SELECT JSONExtract('"-1e3"', 'Nullable(Int64)');
SELECT JSONExtract('"-9.2e18"', 'Nullable(Int64)');
SELECT CAST('{"a":"-9223372036854775808"}' AS JSON(a Int64)).a;
