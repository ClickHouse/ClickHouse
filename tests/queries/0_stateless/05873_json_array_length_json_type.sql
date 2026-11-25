-- JSONArrayLength of a JSON value, a Dynamic subcolumn of JSON or another type returns the same as for the equivalent String.

SELECT 'issue example';
WITH '{"a":{"b":"x"},"d":[1,2]}'::JSON AS j SELECT JSONArrayLength(j), JSONArrayLength(j.d), JSONArrayLength(j.^a);

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
SELECT id, JSONArrayLength(j), JSONArrayLength(j::String) FROM t ORDER BY id;

SELECT 'Dynamic subcolumn vs String';
SELECT
    id,
    dynamicType(j.d) AS dt,
    JSONArrayLength(j.d) AS from_dynamic,
    JSONArrayLength(if(dt = 'String', JSONExtractString(j::String, 'd'), JSONExtractRaw(j::String, 'd'))) AS from_string,
    isNotDistinctFrom(from_dynamic, from_string)
FROM t
WHERE j.d IS NOT NULL
ORDER BY id
SETTINGS dynamic_throw_on_type_mismatch = 1;

SELECT 'sub-object vs String';
SELECT id, JSONArrayLength(j.^d), JSONArrayLength(j.^d::String) FROM t ORDER BY id;

SELECT 'every row of the Dynamic subcolumn, with the default type mismatch behavior';
SELECT count(), countIf(JSONArrayLength(j.d) IS NOT NULL) FROM t SETTINGS dynamic_throw_on_type_mismatch = 1;

DROP TABLE t;

-- { echoOn }
SELECT JSONArrayLength([1, 2, 3]), JSONArrayLength(toJSONString([1, 2, 3]));
SELECT JSONArrayLength([]), JSONArrayLength(toJSONString([]));
SELECT JSONArrayLength([[1], [2, 3]]), JSONArrayLength(toJSONString([[1], [2, 3]]));
SELECT JSONArrayLength(materialize([1, 2])), JSONArrayLength(toJSONString(materialize([1, 2])));
SELECT JSONArrayLength(tuple(1, 'a')), JSONArrayLength(toJSONString(tuple(1, 'a')));
SELECT JSONArrayLength(map('k', 1)), JSONArrayLength(toJSONString(map('k', 1)));
SELECT JSONArrayLength(2), JSONArrayLength(toJSONString(2));
SELECT JSONArrayLength(true), JSONArrayLength(toJSONString(true));

SELECT JSONArrayLength(CAST((1, 'a'), 'Tuple(x UInt8, y String)')), JSONArrayLength(toJSONString(CAST((1, 'a'), 'Tuple(x UInt8, y String)')))
SETTINGS output_format_json_named_tuples_as_objects = 1;
SELECT JSONArrayLength(CAST((1, 'a'), 'Tuple(x UInt8, y String)')), JSONArrayLength(toJSONString(CAST((1, 'a'), 'Tuple(x UInt8, y String)')))
SETTINGS output_format_json_named_tuples_as_objects = 0;

SELECT toTypeName(x), JSONArrayLength(x) FROM (SELECT arrayJoin([NULL, '{"a":1}'])::Nullable(JSON) AS x);
SELECT JSONArrayLength('{"a":[1,2]}'::JSON), JSONArrayLength(materialize('{"a":[1,2]}'::JSON));

SELECT toTypeName(JSONArrayLength('{}'::JSON)), toTypeName(JSONArrayLength([1])), toTypeName(JSONArrayLength(('{"d":[1]}'::JSON).d));
SELECT dynamicElement(JSONArrayLength('[1,2]'::Dynamic), 'UInt64');

-- { echoOff }
