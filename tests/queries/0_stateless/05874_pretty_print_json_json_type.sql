-- Tags: no-fasttest
-- no-fasttest: prettyPrintJSON needs rapidjson

-- prettyPrintJSON of a JSON value, a Dynamic subcolumn of JSON or another type returns the same as for the equivalent String.

SELECT 'issue example';
WITH '{"a":{"b":"x"},"d":[1,2]}'::JSON AS j SELECT prettyPrintJSON(j) FORMAT TSVRaw;
WITH '{"a":{"b":"x"},"d":[1,2]}'::JSON AS j SELECT prettyPrintJSON(j.^a) FORMAT TSVRaw;
WITH '{"a":{"b":"x"},"d":[1,2]}'::JSON AS j SELECT prettyPrintJSON(j.d) FORMAT TSVRaw;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32, j JSON(max_dynamic_paths = 2, max_dynamic_types = 1, `t.n` UInt32)) ENGINE = Memory;
INSERT INTO t VALUES
    (1, '{"a":{"b":"x"},"d":[1,2]}'),
    (2, '{"d":[[1,2],[3]],"e":[{"x":1},{"y":"z"}],"f":0.1,"h":18446744073709551615,"i":true}'),
    (3, '{"d":5,"s":"[1,2]","t":{"n":7}}'),
    (4, '{}'),
    (5, '{"d":"[3,4,5]","e":[]}'),
    (6, '{"d":[{"x":1},{"x":2,"y":[1]}]}'),
    (7, '{"d":"2020-01-01"}'),
    (8, '{"d":true}'),
    (9, '{"d":1.5}'),
    (10, '{"d":{"k":1}}'),
    (11, '{"d":["a","b"]}');

SELECT 'fixture: dynamic paths, shared data, d in the shared variant';
SELECT id, JSONDynamicPaths(j), JSONSharedDataPaths(j), isDynamicElementInSharedData(j.d) FROM t ORDER BY id;

SELECT 'JSON vs String';
SELECT
    id,
    prettyPrintJSON(j) = prettyPrintJSON(j::String),
    prettyPrintJSON(j, 2) = prettyPrintJSON(j::String, 2),
    prettyPrintJSON(j, 0) = prettyPrintJSON(j::String, 0)
FROM t
ORDER BY id;
SELECT id, prettyPrintJSON(j) FROM t WHERE id IN (1, 2) ORDER BY id FORMAT TSVRaw;

SELECT 'sub-object vs String';
SELECT id, prettyPrintJSON(j.^d) = prettyPrintJSON(j.^d::String) FROM t ORDER BY id;

SELECT 'Dynamic subcolumn vs String';
SELECT
    id,
    dynamicType(j.d) AS dt,
    prettyPrintJSON(j.d) = prettyPrintJSON(if(dt = 'String', JSONExtractString(j::String, 'd'), JSONExtractRaw(j::String, 'd')))
FROM t
WHERE j.d IS NOT NULL
ORDER BY id
SETTINGS dynamic_throw_on_type_mismatch = 1;
SELECT id, prettyPrintJSON(j.d) FROM t WHERE id IN (2, 6, 8, 11) ORDER BY id FORMAT TSVRaw;

DROP TABLE t;

SELECT 'typed paths';
DROP TABLE IF EXISTS t_typed;
CREATE TABLE t_typed
(
    id UInt32,
    j JSON(dec Decimal(10, 3), tup Tuple(x UInt8, y String), m Map(String, Array(UInt8)), lc LowCardinality(String), nn Nullable(UInt8), dt64 DateTime64(3, 'UTC'), u UUID)
)
ENGINE = Memory;
INSERT INTO t_typed VALUES
    (1, '{"dec":1.5,"tup":{"x":1,"y":"a"},"m":{"k":[1,2]},"lc":"s","nn":null,"dt64":"2020-01-01 00:00:00.123","u":"00000000-0000-0000-0000-000000000001","other":-0}'),
    (2, '{}');
SELECT id, prettyPrintJSON(j) = prettyPrintJSON(j::String) FROM t_typed ORDER BY id;
SELECT prettyPrintJSON(j) FROM t_typed WHERE id = 1 FORMAT TSVRaw;
DROP TABLE t_typed;

SELECT 'a String value is JSON text, as for the String argument';
WITH '{"s":"bob"}'::JSON AS j SELECT prettyPrintJSON(j.s); -- { serverError BAD_ARGUMENTS }
WITH '{"s":"bob"}'::JSON AS j SELECT prettyPrintJSON(j.s::String); -- { serverError BAD_ARGUMENTS }
WITH '{"s":"bob"}'::JSON AS j SELECT prettyPrintJSON(toJSONString(j.s));

SELECT 'format settings';
WITH '{"h":18446744073709551615}'::JSON AS j SELECT prettyPrintJSON(j), prettyPrintJSON(j) = prettyPrintJSON(j::String)
SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT TSVRaw;
WITH '{"h":18446744073709551615}'::JSON AS j SELECT prettyPrintJSON(j), prettyPrintJSON(j) = prettyPrintJSON(j::String)
SETTINGS output_format_json_quote_64bit_integers = 1 FORMAT TSVRaw;

-- { echoOn }
SELECT prettyPrintJSON([1, 2]), prettyPrintJSON(toJSONString([1, 2]));
SELECT prettyPrintJSON(map('k', [1, 2])), prettyPrintJSON(toJSONString(map('k', [1, 2])));
SELECT prettyPrintJSON(42), prettyPrintJSON(toJSONString(42));
SELECT prettyPrintJSON(tuple(1, 'a')), prettyPrintJSON(toJSONString(tuple(1, 'a')));
SELECT prettyPrintJSON(['a', 'b']), prettyPrintJSON(toJSONString(['a', 'b']));

SELECT prettyPrintJSON('[1,2]'::Variant(String, UInt64)), prettyPrintJSON(5::Variant(String, UInt64));

SELECT toTypeName(x), prettyPrintJSON(x) FROM (SELECT arrayJoin([NULL, '{"a":1}'])::Nullable(JSON) AS x);
SELECT prettyPrintJSON('{"a":1}'::JSON, 33); -- { serverError BAD_ARGUMENTS }

SELECT toTypeName(prettyPrintJSON('{}'::JSON)), toTypeName(prettyPrintJSON(('{"d":[1]}'::JSON).d));
SELECT toTypeName(prettyPrintJSON('{"a":1}'::Dynamic)), prettyPrintJSON('{"a":1}'::Dynamic);
-- { echoOff }
