-- JSONExtractLeafValues and JSONExtractLeafPathsAndValues read a JSON column directly.
-- The result must be the same as for the column cast to String.

-- A DateTime without a time zone is written in the server time zone, the cast to String uses the session time zone.
SET session_timezone = '';

DROP TABLE IF EXISTS t_leaf;
CREATE TABLE t_leaf (id UInt32, j JSON(max_dynamic_paths = 3, `a.b` UInt32, c Nullable(String))) ENGINE = Memory;
INSERT INTO t_leaf VALUES
    (1, '{"a":{"b":7,"x":1},"a-c":"s","d":[1,null,3],"e":[{"f":1,"g":[{"h":"q"}]},{"f":2}],"z":{"y":[[1,2],[3]]},"k1":1,"k2":2.5,"k3":true}'),
    (2, '{"c":"str","k4":"2020-01-01","k5":"2020-01-01 00:00:00","n":null}'),
    (3, '{}'),
    (4, '{"a":{"x":"y"},"d":"not an array","e":[{"f":[{"g":1},2]}],"k6":{"m":[]},"k7":-0.0,"k8":[-0.0,1e300,0.1],"k9":[1,"x",{"b":1}],"":{"":1}}');

SELECT 'typed, dynamic and shared data paths';
SELECT id, JSONDynamicPaths(j), JSONSharedDataPaths(j) FROM t_leaf ORDER BY id;

SELECT 'leaves';
SELECT id, JSONExtractLeafPathsAndValues(j) FROM t_leaf ORDER BY id;
SELECT id, JSONExtractLeafValues(j) FROM t_leaf ORDER BY id;

-- The number of leaves, and the number of rows where the result differs from the String input.
SELECT 'compare with String';
SELECT 'default', sum(length(JSONExtractLeafPathsAndValues(j))), countIf(JSONExtractLeafPathsAndValues(j) != JSONExtractLeafPathsAndValues(j::String)), countIf(JSONExtractLeafValues(j) != JSONExtractLeafValues(j::String)) FROM t_leaf;
SELECT 'empty regex', sum(length(JSONExtractLeafPathsAndValues(j, ''))), countIf(JSONExtractLeafPathsAndValues(j, '') != JSONExtractLeafPathsAndValues(j::String, '')), countIf(JSONExtractLeafValues(j, '') != JSONExtractLeafValues(j::String, '')) FROM t_leaf;
SELECT 'shared data subtrees', sum(length(JSONExtractLeafPathsAndValues(j, '^e\\[0\\]\\.g|^z$'))), countIf(JSONExtractLeafPathsAndValues(j, '^e\\[0\\]\\.g|^z$') != JSONExtractLeafPathsAndValues(j::String, '^e\\[0\\]\\.g|^z$')), countIf(JSONExtractLeafValues(j, '^e\\[0\\]\\.g|^z$') != JSONExtractLeafValues(j::String, '^e\\[0\\]\\.g|^z$')) FROM t_leaf;
SELECT 'dynamic path', sum(length(JSONExtractLeafPathsAndValues(j, '\\.x$'))), countIf(JSONExtractLeafPathsAndValues(j, '\\.x$') != JSONExtractLeafPathsAndValues(j::String, '\\.x$')), countIf(JSONExtractLeafValues(j, '\\.x$') != JSONExtractLeafValues(j::String, '\\.x$')) FROM t_leaf;
SELECT 'intermediate object', sum(length(JSONExtractLeafPathsAndValues(j, '^a$'))), countIf(JSONExtractLeafPathsAndValues(j, '^a$') != JSONExtractLeafPathsAndValues(j::String, '^a$')), countIf(JSONExtractLeafValues(j, '^a$') != JSONExtractLeafValues(j::String, '^a$')) FROM t_leaf;
SELECT 'array element', sum(length(JSONExtractLeafPathsAndValues(j, '^e\\[0\\]$|^d\\[2\\]$'))), countIf(JSONExtractLeafPathsAndValues(j, '^e\\[0\\]$|^d\\[2\\]$') != JSONExtractLeafPathsAndValues(j::String, '^e\\[0\\]$|^d\\[2\\]$')), countIf(JSONExtractLeafValues(j, '^e\\[0\\]$|^d\\[2\\]$') != JSONExtractLeafValues(j::String, '^e\\[0\\]$|^d\\[2\\]$')) FROM t_leaf;
SELECT 'arrays skip', sum(length(JSONExtractLeafPathsAndValues(j, '', 'skip'))), countIf(JSONExtractLeafPathsAndValues(j, '', 'skip') != JSONExtractLeafPathsAndValues(j::String, '', 'skip')), countIf(JSONExtractLeafValues(j, '', 'skip') != JSONExtractLeafValues(j::String, '', 'skip')) FROM t_leaf;
SELECT 'arrays string', sum(length(JSONExtractLeafPathsAndValues(j, '', 'string'))), countIf(JSONExtractLeafPathsAndValues(j, '', 'string') != JSONExtractLeafPathsAndValues(j::String, '', 'string')), countIf(JSONExtractLeafValues(j, '', 'string') != JSONExtractLeafValues(j::String, '', 'string')) FROM t_leaf;
SELECT 'arrays elements', sum(length(JSONExtractLeafPathsAndValues(j, '^k', 'elements'))), countIf(JSONExtractLeafPathsAndValues(j, '^k', 'elements') != JSONExtractLeafPathsAndValues(j::String, '^k', 'elements')), countIf(JSONExtractLeafValues(j, '^k', 'elements') != JSONExtractLeafValues(j::String, '^k', 'elements')) FROM t_leaf;

SELECT 'other types of typed paths';
DROP TABLE IF EXISTS t_leaf_types;
CREATE TABLE t_leaf_types (id UInt32, j JSON(
    dec Decimal(38, 20), i128 Int128, u256 UInt256, uuid UUID, ip IPv4, ip6 IPv6, en Enum8('x' = 1, 'y' = 2),
    t Tuple(UInt8, String), nt Tuple(a Nullable(UInt8), b Array(Tuple(c String))), m Map(String, Array(UInt8)),
    v Variant(UInt8, String), dy Dynamic, f32 Float32, nf Nullable(Float64), lc LowCardinality(String),
    lcn LowCardinality(Nullable(String)), fs FixedString(3), dt64 DateTime64(3, 'UTC'), d32 Date32, ti Time, arr Array(Decimal(10, 2)),
    b Bool, i8 Int8, u16 UInt16, aj Array(JSON), an Array(Nullable(Float32)))) ENGINE = Memory;
INSERT INTO t_leaf_types VALUES
    (1, '{"dec": 0.12345678901234567890, "i128": 170141183460469231731687303715884105727, "u256": 115792089237316195423570985008687907853269984665640564039457584007913129639935, "uuid": "61f0c404-5cb3-11e7-907b-a6006ad3dba0", "ip": "1.2.3.4", "ip6": "::1", "en": "y", "t": [1, "x"], "nt": {"a": null, "b": [{"c": "q"}]}, "m": {"k": [1, 2], "": [3]}, "v": 5, "dy": [1, 2], "f32": 0.1, "nf": "nan", "lc": "low", "lcn": null, "fs": "abc", "dt64": "2020-01-01 00:00:00.123", "d32": "1950-01-01", "ti": "10:00:00", "arr": [1.5, 2.25], "b": true, "i8": -5, "u16": 65535, "aj": [{"x": 1, "y": {"z": [1.5]}}, {}], "an": [0.1, null, -0.0]}'),
    (2, '{"nf": "inf", "f32": -0.0, "v": "str", "dy": {"p": 1}, "lcn": "s"}'),
    (3, '{}');
SELECT id, JSONExtractLeafPathsAndValues(j) FROM t_leaf_types ORDER BY id;
SELECT id, JSONExtractLeafPathsAndValues(j, '', 'string') FROM t_leaf_types WHERE id = 1;
SELECT
    sum(length(JSONExtractLeafPathsAndValues(j))),
    countIf(JSONExtractLeafPathsAndValues(j) != JSONExtractLeafPathsAndValues(j::String)),
    countIf(JSONExtractLeafPathsAndValues(j, '', 'string') != JSONExtractLeafPathsAndValues(j::String, '', 'string')),
    countIf(JSONExtractLeafPathsAndValues(j, '', 'skip') != JSONExtractLeafPathsAndValues(j::String, '', 'skip')),
    countIf(JSONExtractLeafPathsAndValues(j, '^nt|^m\\.|^aj\\[0\\]\\.y') != JSONExtractLeafPathsAndValues(j::String, '^nt|^m\\.|^aj\\[0\\]\\.y'))
FROM t_leaf_types;

SELECT 'values of different types in the shared variant of Dynamic';
DROP TABLE IF EXISTS t_leaf_variant;
CREATE TABLE t_leaf_variant (id UInt32, j JSON(max_dynamic_types = 1)) ENGINE = Memory;
INSERT INTO t_leaf_variant VALUES (1, '{"p": 1}'), (2, '{"p": "s"}'), (3, '{"p": [{"q": 1, "r": [true, 2.5]}]}'), (4, '{"p": [1, 2]}'), (5, '{"p": "2020-01-01"}');
SELECT id, dynamicType(j.p), isDynamicElementInSharedData(j.p), JSONExtractLeafPathsAndValues(j) FROM t_leaf_variant ORDER BY id;
SELECT
    sum(length(JSONExtractLeafPathsAndValues(j))),
    countIf(JSONExtractLeafPathsAndValues(j) != JSONExtractLeafPathsAndValues(j::String)),
    countIf(JSONExtractLeafPathsAndValues(j, '', 'string') != JSONExtractLeafPathsAndValues(j::String, '', 'string')),
    countIf(JSONExtractLeafPathsAndValues(j, '\\.r') != JSONExtractLeafPathsAndValues(j::String, '\\.r'))
FROM t_leaf_variant;

SELECT 'Nullable and constant arguments';
SELECT JSONExtractLeafPathsAndValues(j), toTypeName(JSONExtractLeafPathsAndValues(j)), toTypeName(JSONExtractLeafValues(j))
FROM (SELECT CAST(x, 'Nullable(JSON)') AS j FROM values('x Nullable(String)', ('{"a":{"b":1}}'), (NULL)))
SETTINGS print_pretty_type_names = 0;
SELECT JSONExtractLeafPathsAndValues('{"a":{"b":1},"c":[1,{"d":2}]}'::JSON) = JSONExtractLeafPathsAndValues('{"a":{"b":1},"c":[1,{"d":2}]}'::JSON::String);
SELECT JSONExtractLeafValues(materialize('{"a":{"b":1},"c":[1,{"d":2}]}'::JSON), '^c$');

SELECT 'many rows';
SELECT
    count(),
    sum(length(JSONExtractLeafPathsAndValues(j))),
    countIf(JSONExtractLeafPathsAndValues(j) != JSONExtractLeafPathsAndValues(j::String)),
    countIf(JSONExtractLeafPathsAndValues(j, '^arr\\[1\\]|k3') != JSONExtractLeafPathsAndValues(j::String, '^arr\\[1\\]|k3'))
FROM
(
    SELECT CAST('{"id":' || toString(number) || ',"k' || toString(number % 7) || '":' || toString(number) || ',"arr":[{"x":' || toString(number % 3)
        || ',"y":[' || toString(number) || ',"s"]},{"z":' || if(number % 5 = 0, 'true', 'null') || '}]}', 'JSON(max_dynamic_paths = 4)') AS j
    FROM numbers(1000)
);

SELECT 'session_timezone';
DROP TABLE IF EXISTS t_leaf_tz;
CREATE TABLE t_leaf_tz (id UInt32, j JSON(ts DateTime), INDEX idx JSONExtractLeafValues(j) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
-- A typed and a dynamic path with the same DateTime. The index is built in one session time zone and read in another.
INSERT INTO t_leaf_tz SELECT number, CAST(map('ts', toDateTime(1577836800 + number), 'd', toDateTime(1577836800 + number)), 'JSON(ts DateTime)')
FROM numbers(10) SETTINGS session_timezone = 'America/New_York';
WITH toString(toDateTime(1577836805, 'UTC'), serverTimezone()) AS v
SELECT JSONExtractLeafValues(j) = [v, v] FROM t_leaf_tz WHERE id = 5 SETTINGS session_timezone = 'Asia/Kathmandu';
SELECT count() FROM t_leaf_tz WHERE has(JSONExtractLeafValues(j), toString(toDateTime(1577836805, 'UTC'), serverTimezone()))
SETTINGS session_timezone = 'Asia/Kathmandu', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT count() FROM t_leaf_tz WHERE has(JSONExtractLeafValues(j), toString(toDateTime(1577836805, 'UTC'), serverTimezone()))
SETTINGS session_timezone = 'Asia/Kathmandu', use_skip_indexes = 0, use_query_condition_cache = 0;
SELECT count() FROM t_leaf_tz WHERE has(JSONExtractLeafValues(j), toString(toDateTime(1577836805, 'UTC'), 'Asia/Kathmandu'))
SETTINGS session_timezone = 'Asia/Kathmandu', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT count() FROM t_leaf_tz WHERE has(JSONExtractLeafValues(j), toString(toDateTime(1577836805, 'UTC'), 'Asia/Kathmandu'))
SETTINGS session_timezone = 'Asia/Kathmandu', use_skip_indexes = 0, use_query_condition_cache = 0;

DROP TABLE t_leaf;
DROP TABLE t_leaf_types;
DROP TABLE t_leaf_variant;
DROP TABLE t_leaf_tz;
