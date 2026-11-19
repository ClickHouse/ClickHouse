-- `JSONExtract` and a `JSON` typed path must not wrap or truncate an integer outside the range of `Time`.

SELECT JSONExtract('{"t":18446744073709551615}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":4294967297}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-4294967297}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":2147483648}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":4294967297}', 't', 'Time');
SELECT JSONExtract(materialize('{"t":4294967297}'), 't', 'Nullable(Time)');
SELECT JSONExtractKeysAndValues('{"a":18446744073709551615,"b":4294967297,"c":45045}', 'Nullable(Time)');

SELECT CAST('{"t":4294967297}' AS JSON(t Time)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"t":18446744073709551615}' AS JSON(t Time)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"t":4294967297}' AS JSON(t Time)) SETTINGS type_json_skip_invalid_typed_paths = 1;

-- Values that already behaved correctly.
SELECT JSONExtract('{"t":45045}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-45045}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":"12:30:45"}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":2147483647}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-2147483648}', 't', 'Nullable(Time)');
SELECT CAST('{"t":45045}' AS JSON(t Time));

-- Input formats must not wrap an integer outside the range of `Time` either.
SELECT * FROM format(JSONEachRow, 't Time', '{"t":4294967297}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(JSONEachRow, 't Time', '{"t":18446744073709551615}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(JSONEachRow, 't Time', '{"t":-4294967297}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CustomSeparated, 't Time', '4294967297\n') SETTINGS format_custom_escaping_rule = 'Quoted'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(Values, 't Time', '(4294967297)');
SELECT * FROM format(JSONEachRow, 't Time', '{"t":45045}');
SELECT * FROM format(Values, 't Time', '(45045)');
