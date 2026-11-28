-- An ignored key of a named `Tuple` is skipped like an unknown top-level field,
-- so it may hold the MongoDB shell `ISODate("...")` wrapper.

SET session_timezone = 'UTC';
SET input_format_json_ignore_unknown_keys_in_named_tuple = 1;

SELECT * FROM format(JSONEachRow, 'v Tuple(id UInt8)', '{"v":{"id":1,"ts":ISODate("2024-05-29T23:16:12.256Z")}}');
SELECT * FROM format(JSONEachRow, 'v Tuple(id UInt8)', '{"v":{"ts":new ISODate("2024-05-29T23:16:12.256Z"),"id":2}}');
SELECT * FROM format(JSONEachRow, 'v Tuple(id UInt8)', '{"v":{"id":3,"nested":{"ts":[ISODate("2024-05-29T23:16:12.256Z")]}}}');

-- A named `Tuple` inside `Variant`, with and without a `DateTime64` variant.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Tuple(id UInt8), String)', '{"v":{"id":4,"ts":ISODate("2024-05-29T23:16:12.256Z")}}\n{"v":"str"}') ORDER BY variantType(v);
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Tuple(id UInt8), DateTime64(3))', '{"v":{"id":5,"ts":ISODate("2024-05-29T23:16:12.256Z")}}\n{"v":ISODate("2024-05-29T23:16:12.256Z")}') ORDER BY variantType(v);

-- A `String` variant must not take an object with the wrapper as a raw string.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Tuple(id UInt8), String)', '{"v":{"x":1,"ts":ISODate("2024-05-29T23:16:12.256Z")}}');

-- Without the setting the wrapper is not accepted.
SELECT * FROM format(JSONEachRow, 'v Tuple(id UInt8)', '{"v":{"id":1,"ts":ISODate("2024-05-29T23:16:12.256Z")}}') SETTINGS input_format_json_ignore_unknown_keys_in_named_tuple = 0; -- { serverError INCORRECT_DATA }
SELECT * FROM format(JSONEachRow, 'v Variant(Tuple(id UInt8), String)', '{"v":{"id":1,"ts":ISODate("2024-05-29T23:16:12.256Z")}}') SETTINGS input_format_json_ignore_unknown_keys_in_named_tuple = 0; -- { serverError INCORRECT_DATA }
