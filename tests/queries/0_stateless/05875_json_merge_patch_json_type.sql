-- Tags: no-fasttest
-- Reason: needs RapidJSON, which is not enabled in the fast test build.

-- JSONMergePatch with a JSON first argument returns the same JSON type. The results are compared with the String
-- route, JSONMergePatch(c::String, ...) parsed back into the type, wherever that route can round-trip the row.

SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_plain (id UInt32, c JSON) ENGINE = Memory;
INSERT INTO t_plain VALUES
    (1, '{"k":1,"o":{"p":1,"q":2},"arr":[1,2],"objs":[{"m":1},{"m":2}],"d":"2020-01-01","s":"str"}'),
    (2, '{"k":"x","o":5,"z":[1,2,3],"b":true,"f":0.5}'),
    (3, '{}'),
    (4, '{"o":{"q":{"deep":1}},"x":{"y":1},"n":-5}');

SELECT 'plain, constant patches';
SELECT id, JSONMergePatch(c, '{"k":100}') AS r, toTypeName(r), toString(r) = toString(JSONMergePatch(c::String, '{"k":100}')::JSON) FROM t_plain ORDER BY id;
SELECT id, JSONMergePatch(c, '{"k":null,"o":{"q":null,"r":7}}') AS r, toString(r) = toString(JSONMergePatch(c::String, '{"k":null,"o":{"q":null,"r":7}}')::JSON) FROM t_plain ORDER BY id;
SELECT id, JSONMergePatch(c, '{"o":"scalar","new":{"deep":[1,2,3]}}') AS r, toString(r) = toString(JSONMergePatch(c::String, '{"o":"scalar","new":{"deep":[1,2,3]}}')::JSON) FROM t_plain ORDER BY id;
SELECT id, JSONMergePatch(c, '{"arr":[{"m":1}],"objs":null,"z":{"y":{"w":1}}}') AS r, toString(r) = toString(JSONMergePatch(c::String, '{"arr":[{"m":1}],"objs":null,"z":{"y":{"w":1}}}')::JSON) FROM t_plain ORDER BY id;

SELECT 'plain, non-constant patches';
SELECT p, count(), countIf(toString(JSONMergePatch(c, p)) = toString(JSONMergePatch(c::String, p)::JSON))
FROM t_plain, (SELECT arrayJoin([
    '{}', '{"k":100}', '{"new":{"deep":[1,2,3]}}', '{"k":null}', '{"o":null}', '{"o":{"q":null,"r":7}}', '{"o":"scalar"}',
    '{"arr":[{"m":1}],"objs":null}', '{"z":{"y":{"w":1}}}', '{"a.b":1}', '{"x":{}}', '{"k":1,"k":2}', '{"o":{"p":1},"o":{"q":2}}']) AS p) AS patches
GROUP BY p ORDER BY p;

SELECT 'plain, several patches';
SELECT id, JSONMergePatch(c, '{"k":1}', '{"k":null,"m":2}', '{"m":{"n":3}}') AS r,
    toString(r) = toString(JSONMergePatch(c::String, '{"k":1}', '{"k":null,"m":2}', '{"m":{"n":3}}')::JSON)
FROM t_plain ORDER BY id;
SELECT id, JSONMergePatch(c) AS r, toString(r) = toString(c) FROM t_plain ORDER BY id;

SELECT 'plain, JSON patches';
SELECT id, JSONMergePatch(c, '{"k":7,"o":{"p":8}}'::JSON) AS r, toString(r) = toString(JSONMergePatch(c::String, '{"k":7,"o":{"p":8}}')::JSON) FROM t_plain ORDER BY id;
SELECT id, JSONMergePatch(c, CAST('{"k":null}', 'JSON(k Nullable(Int64))')) AS r FROM t_plain ORDER BY id;
SELECT p, count(), countIf(toString(JSONMergePatch(c, p::JSON)) = toString(JSONMergePatch(c::String, p::JSON::String)::JSON))
FROM t_plain, (SELECT arrayJoin(['{"k":7}', '{"o":{"p":8},"n":{"m":1}}', '{"s":[1,2]}']) AS p) AS patches
GROUP BY p ORDER BY p;

SELECT 'constant JSON first argument';
SELECT p, JSONMergePatch('{"a":1,"b":{"c":2}}'::JSON, p) AS r, toString(r) = toString(JSONMergePatch('{"a":1,"b":{"c":2}}', p)::JSON)
FROM (SELECT arrayJoin(['{"a":null}', '{"b":{"c":3}}']) AS p) ORDER BY p;

SELECT 'Nullable(JSON)';
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (id UInt32, c Nullable(JSON)) ENGINE = Memory;
INSERT INTO t_nullable VALUES (1, '{"a":1}'), (2, NULL);
SELECT id, JSONMergePatch(c, '{"b":2}') AS r, toTypeName(r) FROM t_nullable ORDER BY id;

SELECT 'typed paths and shared data';
DROP TABLE IF EXISTS t_typed;
CREATE TABLE t_typed (id UInt32, j JSON(max_dynamic_paths = 2, `t.a` UInt32, `t.n` Nullable(String), `t.e` Enum8('x' = 1, 'y' = 2))) ENGINE = Memory;
INSERT INTO t_typed VALUES
    (1, '{"t":{"a":1,"n":"s","e":"y"},"p1":1,"p2":2,"p3":3,"p4":{"q":4}}'),
    (2, '{"t":{"a":2},"p2":"v","p5":[1],"p6":{"r":{"s":1}}}'),
    (3, '{"p1":{"x":1},"p3":"w","k":5}'),
    (4, '{}');
SELECT countIf(length(JSONSharedDataPaths(j)) > 0) > 0, countIf(length(JSONDynamicPaths(j)) > 0) > 0 FROM t_typed;
SELECT toTypeName(JSONMergePatch(j, '{}')) = toTypeName(j) FROM t_typed LIMIT 1;
SELECT id, JSONMergePatch(j, '{"t":{"n":null,"a":null,"e":"y"},"p3":null,"n1":1,"n2":2,"n3":{"n4":3}}') AS r,
    toString(r) = toString(CAST(JSONMergePatch(j::String, '{"t":{"n":null,"a":null,"e":"y"},"p3":null,"n1":1,"n2":2,"n3":{"n4":3}}'), toTypeName(j)))
FROM t_typed ORDER BY id;
SELECT p, count(), countIf(toString(JSONMergePatch(j, p)) = toString(CAST(JSONMergePatch(j::String, p), toTypeName(j))))
FROM t_typed, (SELECT arrayJoin([
    '{}', '{"k":100}', '{"k":null}', '{"t":{"a":42}}', '{"t":null}', '{"t":{"n":null,"a":null,"e":"y"}}', '{"t":{"a":"7"}}',
    '{"p3":null}', '{"p3":33}', '{"p4":{"q":null}}', '{"p6":{"r":5}}', '{"p1":{"x":2,"y":3}}', '{"n1":1,"n2":2,"n3":{"n4":3}}',
    '{"o":{"q":null,"r":7}}', '{"p2":{}}']) AS p) AS patches
GROUP BY p ORDER BY p;

SELECT 'objects on Map and Tuple typed paths';
DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (id UInt32, c JSON(m Map(String, UInt8), u Tuple(a UInt8, b String))) ENGINE = Memory;
INSERT INTO t_map VALUES (1, '{"m":{"x":1},"u":{"a":1,"b":"z"},"k":1}'), (2, '{"k":2}');
SELECT id, p, JSONMergePatch(c, p) AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c)))
FROM t_map, (SELECT arrayJoin(['{"m":{"y":2}}', '{"m":{"x":null}}', '{"u":{"a":9}}', '{"m":null}', '{"other":1}']) AS p) AS patches
ORDER BY id, p;
SELECT id, JSONMergePatch(c, '{"m":{"y":2}}') AS r FROM t_map ORDER BY id;
SELECT p, JSONMergePatch(c, p) AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c)))
FROM (SELECT CAST('{"m":{"x":1,"y":2}}', 'JSON(m Map(String, UInt8))') AS c) AS src,
    (SELECT arrayJoin(['{"m":{"x":3}}', '{"m":{"x":null}}', '{"m":{"y":3,"z":4}}']) AS p) AS patches ORDER BY p;
SELECT JSONMergePatch(c, '{"m":{"x":3}}') AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, '{"m":{"x":3}}'), toTypeName(c)))
FROM (SELECT CAST('{"m":{"x":1,"x":2}}', 'JSON(m Map(String, UInt8))') AS c);
SELECT p, JSONMergePatch(c, p) AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c)))
FROM (SELECT CAST('{"m":{"x":1,"x":{"a":2},"y":0}}', 'JSON(m Map(String, Dynamic))') AS c) AS src,
    (SELECT arrayJoin(['{"m":{"x":3}}', '{"m":{"x":{"b":4}}}']) AS p) AS patches ORDER BY p;
SELECT JSONMergePatch(c, '{"m":{"x":3}}') AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, '{"m":{"x":3}}'), toTypeName(c)))
FROM (SELECT CAST('{"m":{"x":1,"x":{"a":2},"y":0}}', 'JSON(m Map(String, Variant(UInt8, Map(String, UInt8))))') AS c);

SELECT 'round-trip settings';
SELECT JSONAllPathsWithTypes(JSONMergePatch(c, '{"m":{"y":2}}'))['n'] FROM (SELECT CAST('{"m":{"x":1},"n":42}', 'JSON(m Map(String, UInt8))') AS c) SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT JSONAllPathsWithTypes(JSONMergePatch('{"a":1}'::JSON, '{"k":5}'::JSON))['k'] SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT JSONAllPathsWithTypes(JSONMergePatch('{"a":1}'::JSON, '{"k":0.5}'::JSON))['k'] SETTINGS output_format_json_quote_64bit_floats = 1;
SELECT id, r.u FROM (SELECT id, JSONMergePatch(c, '{"u":{"a":9}}') AS r FROM t_map) ORDER BY id SETTINGS output_format_json_named_tuples_as_objects = 0;
SELECT id, r.m FROM (SELECT id, JSONMergePatch(c, '{"m":{"y":2}}') AS r FROM t_map) ORDER BY id SETTINGS output_format_json_map_as_array_of_tuples = 1;
SELECT JSONAllPathsWithTypes(r)['t'] FROM (SELECT JSONMergePatch(c, '{"m":{"y":2}}') AS r FROM (SELECT CAST('{"t":"2020-01-01 10:00:00","m":{"x":1}}', 'JSON(m Map(String, UInt8))') AS c)) SETTINGS date_time_output_format = 'unix_timestamp';
SELECT JSONAllPathsWithTypes(CAST(c, 'JSON(b UInt32)'))['a'] FROM (SELECT '{"a":0.5,"b":1}'::JSON AS c) SETTINGS json_use_optimized_type_conversion = 0, output_format_json_quote_64bit_floats = 1;
SELECT JSONAllPathsWithTypes(JSONMergePatch('{"a":1}'::JSON, CAST('{"k":1.5}', 'JSON(k Decimal(10, 2))')))['k'] SETTINGS output_format_json_quote_decimals = 1;
SELECT JSONAllPathsWithTypes(CAST(c, 'JSON(b UInt32)'))['k'] FROM (SELECT CAST('{"k":1.5,"b":1}', 'JSON(k Decimal(10, 2))') AS c) SETTINGS json_use_optimized_type_conversion = 0, output_format_json_quote_decimals = 1;

SELECT 'stored types of untouched paths';
SET input_format_try_infer_dates = 0, input_format_try_infer_datetimes = 0;
DROP TABLE IF EXISTS t_types;
CREATE TABLE t_types (c JSON(m Map(String, UInt8))) ENGINE = Memory;
INSERT INTO t_types VALUES ('{"d":"2020-01-01","k":1,"m":{"x":1}}');
SET input_format_try_infer_dates = 1, input_format_try_infer_datetimes = 1;
SELECT p, JSONAllPathsWithTypes(JSONMergePatch(c, p))['d'] FROM t_types,
    (SELECT arrayJoin(['{"k":2}', '{"k":null}', '{"n":{"o":1}}', '{"m":{"y":2}}', '{"k":1,"k":2}']) AS p) AS patches ORDER BY p;
SELECT JSONAllPathsWithTypes(JSONMergePatch(c, '{"k":2}'))['d'] FROM t_types;
DROP TABLE t_types;

SELECT 'SKIP rules';
SELECT p, JSONMergePatch(c, p) AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c)))
FROM (SELECT CAST('{"a":1,"s":{"x":1},"r1":2,"k":{"q":1}}', 'JSON(SKIP s, SKIP REGEXP \'r.*\')') AS c) AS src,
    (SELECT arrayJoin(['{"s":{"y":1}}', '{"r2":5,"k":{"q":null}}', '{"s":null,"a":{"s":1}}']) AS p) AS patches
ORDER BY p;

SELECT 'max_dynamic_paths = 0';
SELECT p, JSONMergePatch(c, p) AS r, toString(r) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c))), JSONSharedDataPaths(r)
FROM (SELECT CAST('{"a":1,"b":{"c":2}}', 'JSON(max_dynamic_paths = 0)') AS c) AS src,
    (SELECT arrayJoin(['{"a":null,"n":1}', '{"b":{"c":3,"d":4}}']) AS p) AS patches
ORDER BY p;

SELECT 'parsing limit on dynamic paths';
DROP TABLE IF EXISTS t_limit;
CREATE TABLE t_limit (c JSON) ENGINE = Memory;
INSERT INTO t_limit VALUES ('{"z":0}');
SELECT length(JSONDynamicPaths(r)), length(JSONSharedDataPaths(r)), toString(r)
FROM (SELECT JSONMergePatch(c, '{"a":1,"b":2,"c":3}') AS r FROM t_limit) SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;
SELECT length(JSONDynamicPaths(x)), length(JSONSharedDataPaths(x)), toString(x)
FROM (SELECT arrayJoin([r, r]) AS x FROM (SELECT JSONMergePatch(c, '{"a":1,"b":2,"c":3}') AS r FROM t_limit)) SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 1;
DROP TABLE t_limit;

SELECT 'escaped dots in keys';
SELECT JSONMergePatch(c, '{"a.b":5}') AS r, toString(r) = toString(JSONMergePatch(c::String, '{"a.b":5}')::JSON)
FROM (SELECT '{"a.b":1,"a":{"b":2}}'::JSON AS c) SETTINGS json_type_escape_dots_in_keys = 1;

SELECT 'literal and nested object under the same key';
SELECT JSONMergePatch(c, '{"a":{"b":"x"}}') AS r, JSONMergePatch(c, '{"c":1}') AS r2,
    toString(r2) = toString(CAST(JSONMergePatch(c::String, '{"c":1}'), toTypeName(c)))
FROM (SELECT CAST('{"a":5,"a":{"b":"y"}}', 'JSON(a UInt32, `a.b` String)') AS c)
SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 1;
SELECT p, JSONMergePatch(c, p) FROM (SELECT '{"k0":0,"k0":{"x":0}}'::JSON AS c) AS src, (SELECT arrayJoin(['{"k0":null}', '{"k0":5}', '{"k0":{"y":1}}']) AS p) AS patches ORDER BY p;
SELECT p, JSONMergePatch(c, p) FROM (SELECT CAST('{"k0":0,"k0":{"x":0},"m":{"x":1}}', 'JSON(m Map(String, UInt8))') AS c) AS src,
    (SELECT arrayJoin(['{"m":{"y":2},"k0":5}', '{"m":{"y":2},"k0":null}', '{"m":{"y":2},"k0":{"y":1}}']) AS p) AS patches ORDER BY p;

SELECT 'many rows';
DROP TABLE IF EXISTS t_many;
CREATE TABLE t_many (id UInt32, c JSON(max_dynamic_paths = 4), p String) ENGINE = Memory;
INSERT INTO t_many SELECT number,
    format('{{"k{}":{},"o{}":{{"x":{},"y":"s{}"}},"g":{}}}', toString(number % 7), toString(number), toString(number % 5), toString(number % 3), toString(number), toString(number % 2)),
    format('{{"k{}":null,"o{}":{{"x":{},"z":[{}]}},"o{}":null,"n{}":{},"k{}":{{"w":{{"v":{}}}}},"o{}":"s"}}',
        toString(number % 3), toString((number + 1) % 5), toString(number % 4), toString(number), toString((number + 3) % 5),
        toString(number % 6), toString(number), toString(number % 7), toString(number), toString(number % 5))
FROM numbers(1000);
SELECT count(), countIf(toString(JSONMergePatch(c, p)) = toString(CAST(JSONMergePatch(c::String, p), toTypeName(c)))) FROM t_many;
SELECT count(), countIf(toString(JSONMergePatch(c, '{"k1":null,"o2":{"x":0},"g":[1]}')) = toString(CAST(JSONMergePatch(c::String, '{"k1":null,"o2":{"x":0},"g":[1]}'), toTypeName(c)))) FROM t_many;

SELECT 'ALTER UPDATE';
DROP TABLE IF EXISTS test_json_update;
CREATE TABLE test_json_update (c JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_update VALUES ('{"key": 1, "value": 1}');
ALTER TABLE test_json_update UPDATE c = jsonMergePatch(c, '{"value": 100}') WHERE c.key = 1 SETTINGS mutations_sync = 2;
SELECT c FROM test_json_update;

DROP TABLE IF EXISTS t_alter_json;
DROP TABLE IF EXISTS t_alter_string;
CREATE TABLE t_alter_json (id UInt32, c JSON(max_dynamic_paths = 4)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_alter_string (id UInt32, c JSON(max_dynamic_paths = 4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter_json SELECT id, c FROM t_many;
INSERT INTO t_alter_string SELECT id, c FROM t_many;
ALTER TABLE t_alter_json UPDATE c = JSONMergePatch(c, '{"k1":null,"o2":{"x":0},"g":[1],"new1":1,"new2":{"a":2}}') WHERE id % 3 != 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_alter_string UPDATE c = JSONMergePatch(c::String, '{"k1":null,"o2":{"x":0},"g":[1],"new1":1,"new2":{"a":2}}')::JSON WHERE id % 3 != 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_alter_json AS a INNER JOIN t_alter_string AS b USING id WHERE toString(a.c) != toString(b.c);
SELECT count() FROM t_alter_json WHERE c.new1 = 1;

SELECT 'lightweight UPDATE';
DROP TABLE IF EXISTS t_lwu;
CREATE TABLE t_lwu (id UInt32, c JSON) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu VALUES (1, '{"key":1,"value":1}'), (2, '{"key":2,"value":2}');
UPDATE t_lwu SET c = JSONMergePatch(c, '{"value": 100, "key": null}') WHERE id = 1;
SELECT id, c FROM t_lwu ORDER BY id;

SELECT 'errors';
SELECT JSONMergePatch(c, 'not json') FROM t_plain; -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(c, '[1]') FROM t_plain; -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(c, '{"arr":[{"m":1,"m":2}]}') FROM t_plain; -- { serverError INCORRECT_DATA }
SELECT JSONMergePatch(c::String, '{"arr":[{"m":1,"m":2}]}')::JSON FROM t_plain; -- { serverError INCORRECT_DATA }
SELECT JSONMergePatch(c, 1) FROM t_plain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_plain;
DROP TABLE t_nullable;
DROP TABLE t_typed;
DROP TABLE t_map;
DROP TABLE t_many;
DROP TABLE test_json_update;
DROP TABLE t_alter_json;
DROP TABLE t_alter_string;
DROP TABLE t_lwu;
