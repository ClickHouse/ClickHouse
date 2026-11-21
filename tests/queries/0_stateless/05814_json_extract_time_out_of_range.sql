-- `JSONExtract` and a `JSON` typed path must not store an integer outside the range of `Time` (-999:59:59 to 999:59:59).

SELECT JSONExtract('{"t":18446744073709551615}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":4294967297}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-4294967297}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":2147483648}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":2147483647}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-2147483648}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":3600000}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-3600000}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":4294967297}', 't', 'Time');
SELECT JSONExtract(materialize('{"t":4294967297}'), 't', 'Nullable(Time)');
SELECT JSONExtractKeysAndValues('{"a":18446744073709551615,"b":4294967297,"c":45045,"d":3600000}', 'Nullable(Time)');

SELECT CAST('{"t":4294967297}' AS JSON(t Time)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"t":18446744073709551615}' AS JSON(t Time)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"t":3600000}' AS JSON(t Time)); -- { serverError INCORRECT_DATA }
SELECT CAST('{"t":4294967297}' AS JSON(t Time)) SETTINGS type_json_skip_invalid_typed_paths = 1;

-- Values that already behaved correctly.
SELECT JSONExtract('{"t":45045}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-45045}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":"12:30:45"}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":3599999}', 't', 'Nullable(Time)');
SELECT JSONExtract('{"t":-3599999}', 't', 'Nullable(Time)');
SELECT CAST('{"t":45045}' AS JSON(t Time));
SELECT CAST('{"t":3599999}' AS JSON(t Time));
