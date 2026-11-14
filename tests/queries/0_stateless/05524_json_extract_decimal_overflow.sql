-- `JSONExtract` of a number out of the range of `Decimal` or `Time64` must return the default value
-- (NULL for `Nullable`), as it does for other types and for the same number given as a string,
-- instead of throwing `DECIMAL_OVERFLOW`.

SELECT JSONExtract('{"a":1e300}', 'a', 'Nullable(Decimal(10, 2))');
SELECT JSONExtract('{"a":1e300}', 'a', 'Decimal(10, 2)');
SELECT JSONExtract('{"a":1e300}', 'a', 'Decimal32(2)');
SELECT JSONExtract('{"a":1e40}', 'a', 'Decimal(38, 2)');
SELECT JSONExtract(materialize('{"a":1e300}'), 'a', 'Nullable(Decimal(10, 2))');
SELECT JSONExtract('{"a":18446744073709551615}', 'a', 'Nullable(Decimal(9, 2))');
SELECT JSONExtract('{"a":-123456789012345678}', 'a', 'Decimal(10, 2)');
SELECT JSONExtract('{"a":1e300}', 'a', 'Variant(Decimal(10, 2), String)');
SELECT JSONExtractKeysAndValues('{"a":1e300,"b":1.5}', 'Nullable(Decimal(10, 2))');
SELECT JSONExtract('{"a":1e300}', 'a', 'Nullable(Time64(3))');
SELECT JSONExtract('{"a":1e300}', 'a', 'Time64(3)');

-- The `JSON` type reports a value that does not fit a typed path as invalid data, as it does for the string form.
SELECT CAST('{"a":1e300}' AS JSON(a Nullable(Decimal(10, 2)))); -- { serverError INCORRECT_DATA }

-- Values that already behaved correctly.
SELECT JSONExtract('{"a":"1e300"}', 'a', 'Nullable(Decimal(10, 2))');
SELECT JSONExtract('{"a":1e300}', 'a', 'Nullable(Int32)');
SELECT JSONExtract('{"a":12345678.91}', 'a', 'Nullable(Decimal(10, 2))');
SELECT JSONExtract('{"a":-1234}', 'a', 'Decimal(10, 2)');
SELECT JSONExtract('{"a":4294967295}', 'a', 'Decimal(18, 2)');
SELECT JSONExtract('{"a":1.5}', 'a', 'Time64(3)');
