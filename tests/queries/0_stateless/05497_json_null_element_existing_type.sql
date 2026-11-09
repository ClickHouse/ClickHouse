-- A null element in a value on a dynamic JSON path gets its own inferred type,
-- like a first value would, instead of being replaced by the default of an existing type.

SELECT 'Array of arrays';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [null]}')))
ORDER BY id;

SELECT 'Array of objects';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [{"k": 1}]}'), (2, '{"a": [null]}')))
ORDER BY id;

SELECT 'Nested path in array of objects';
SELECT id, toString(j), dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [{"k": [[1]]}]}'), (2, '{"a": [{"k": [null]}]}')))
ORDER BY id;

SELECT 'Tuple';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [1, "x", [2]]}'), (2, '{"a": [1, "x", null]}')))
ORDER BY id
SETTINGS input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;

SELECT 'Incomplete type';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [null]}')))
ORDER BY id
SETTINGS input_format_json_infer_incomplete_types_as_strings = 0; -- { serverError INCORRECT_DATA }

SELECT 'Null that fits the inferred type';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [[1], null]}')))
ORDER BY id;

SELECT 'Reversed order';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": [null]}'), (2, '{"a": [[1]]}')))
ORDER BY id;

SELECT 'Shared data';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON(max_dynamic_paths = 0) AS j FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [null]}')))
ORDER BY id;

SELECT 'JSONExtract into Dynamic';
SELECT id, d, dynamicType(d) FROM (SELECT id, JSONExtract(json, 'a', 'Dynamic') AS d FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [null]}')))
ORDER BY id;

SELECT 'Typed path';
SELECT id, j.a, toTypeName(j.a) FROM (SELECT id, json::JSON(a Array(Array(Int64))) AS j FROM values('id UInt8, json String',
    (1, '{"a": [[1]]}'), (2, '{"a": [null]}')))
ORDER BY id;
