-- Tags: no-fasttest

-- A value absent from the JSON input gets the default of its type, so an Enum gets its first element, not 0.

DROP TABLE IF EXISTS t_json_enum_tuple;
CREATE TABLE t_json_enum_tuple (id UInt32, j JSON(t Tuple(n UInt8, status Enum8('a' = 1, 'b' = 2)))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_enum_tuple FORMAT JSONEachRow {"id": 1, "j": {"t": {"n": 5}}} {"id": 2, "j": {"t": null}} {"id": 3, "j": {"t": [7]}} {"id": 4, "j": {"t": {"n": 8, "status": "b"}}}
;

SELECT * FROM t_json_enum_tuple ORDER BY id;
DROP TABLE t_json_enum_tuple;

SELECT j.a, j.m, j.nested FROM format(JSONEachRow, 'j JSON(a Array(Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))), m Map(String, Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))), nested Tuple(n UInt8, inner Tuple(status Enum8(\'a\' = 1, \'b\' = 2))))', '{"j": {"a": [{"n": 1}], "m": {"k": {"n": 2}}, "nested": {"n": 3}}}');
SELECT j.t FROM format(JSONEachRow, 'j JSON(t Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2)))', '{"j": {"t": {}}}'); -- { serverError INCORRECT_DATA }

SELECT JSONExtract('{}', 'status', 'Enum8(\'a\' = 1, \'b\' = 2)'), JSONExtract('{"status": "zzz"}', 'status', 'Enum8(\'a\' = 1, \'b\' = 2)');
SELECT JSONExtract('{}', 's', 'Enum8(\'n\' = -1, \'z\' = 0)');
SELECT JSONExtract('{}', 'Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('{"n": 5, "status": "zzz"}', 'Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('[5, "zzz"]', 'Tuple(UInt8, Enum8(\'a\' = 1, \'b\' = 2))'), JSONExtract('{"x": 5, "y": "zzz"}', 'Tuple(UInt8, Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('[{"n": 5}, {}]', 'Array(Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2)))');
SELECT JSONExtract('["b", "zzz"]', 'Array(Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('{"k": "zzz", "k2": "b"}', 'Map(String, Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtractKeysAndValues('{"k": {"n": 5}}', 'Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('[1]', 'JSON(e Enum8(\'a\' = 1, \'b\' = 2))').e;
SELECT JSONExtract('{"n": 5}', 'Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))') SETTINGS allow_simdjson = 0;
SELECT JSONExtract('{}', 'Nullable(Tuple(a Int32))'), JSONExtract('{}', 'status', 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))');
SELECT JSONExtract('{}', 'Variant(Tuple(a Int32), Map(String, Int32))') AS v, variantType(v);
SELECT JSONExtract('{"n": 5}', 'Tuple(n UInt8, s String)'), JSONType('{}', 'x');
