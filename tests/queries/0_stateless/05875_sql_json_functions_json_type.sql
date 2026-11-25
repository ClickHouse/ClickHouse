-- JSON_EXISTS, JSON_VALUE and JSON_QUERY on the JSON type must return what they return on the same value cast to String.

SELECT JSON_VALUE('{"a":{"b":"x"}}'::JSON, '$.a.b');

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_memory;
DROP VIEW IF EXISTS oracle;

CREATE TABLE t (id UInt32, j JSON(max_dynamic_paths = 2, a.b UInt32, c Nullable(String), t Tuple(x UInt8, y String), SHARED s UInt64))
ENGINE = Memory;
INSERT INTO t VALUES
    (1, '{"a":{"b":1,"x":"one"},"c":"c1","arr":[1,2,null,4],"ao":[{"k":1},{"k":2,"m":[1,2]}],"d":{"e":{"f":"deep"}},"n":null}'),
    (2, '{"p":1,"p.q":2,"a-c":"dash","a":{"x":{"y":[1,{"z":2}]}},"f":[1e300,-0.0,0.1,18446744073709551615],"str":"a/b\\"q\\u0001"}'),
    (3, '{"t":{"x":7,"y":"ty"},"s":5,"k1":{"k2":[[1,2],[3]]},"dt":"2020-01-01 10:00:00","date":"2020-01-01","b":true}'),
    (4, '{}'),
    (5, '{"ao":[],"arr":"not an array","c":null}');

SELECT 'storage';
SELECT id, JSONDynamicPaths(j), JSONSharedDataPaths(j) FROM t ORDER BY id;

SELECT 'values';
SELECT id, JSON_VALUE(j, '$.a.b'), JSON_QUERY(j, '$.a'), JSON_QUERY(j, '$.p'), JSON_VALUE(j, '$.p.q'), JSON_QUERY(j, '$.ao[*].k'),
    JSON_QUERY(j, '$.arr[1 to 3]'), JSON_EXISTS(j, '$.d.e'), JSON_QUERY(j, '$.f')
FROM t ORDER BY id;

-- Per row: the paths where the two inputs differ, then how many paths JSON_QUERY found.
CREATE VIEW oracle AS
WITH ['$', '$.a', '$.a.b', '$.a.x', '$.a.x.y[1].z', '$.c', '$.missing', '$.a.b.c', '$.p', '$.p.q', '$["p.q"]', '$["a-c"]', '$.arr',
    '$.arr[0]', '$.arr[1 to 3]', '$.arr[0, 2]', '$.arr[*]', '$.ao', '$.ao[*].k', '$.ao[1].m[0]', '$.ao[*]', '$.d', '$.d.e', '$.d.e.f',
    '$.d[*]', '$.d[*].e', '$.d[0]', '$[*]', '$[*].a', '$.f[*]', '$.str', '$.t', '$.t.x', '$.s', '$.k1.k2[*][0]', '$.dt', '$.date', '$.b',
    '$.n'] AS P
SELECT id,
    arrayFilter((p, x, y) -> x != y, P, JSON_EXISTS(j, P), JSON_EXISTS(j::String, P)) AS exists_differ,
    arrayFilter((p, x, y) -> NOT (x <=> y), P, JSON_VALUE(j, P), JSON_VALUE(j::String, P)) AS value_differ,
    arrayFilter((p, x, y) -> x != y, P, JSON_QUERY(j, P), JSON_QUERY(j::String, P)) AS query_differ,
    length(arrayFilter(x -> x != '', JSON_QUERY(j, P))) AS found
FROM t;

SELECT 'oracle';
SELECT * FROM oracle ORDER BY id;
SELECT 'allow_complex';
SELECT * FROM oracle ORDER BY id SETTINGS function_json_value_return_type_allow_complex = 1;
SELECT 'allow_nullable';
SELECT * FROM oracle ORDER BY id SETTINGS function_json_value_return_type_allow_nullable = 1;
SELECT 'skip_null_typed_paths';
SELECT * FROM oracle ORDER BY id SETTINGS type_json_skip_null_typed_paths = 1;
SELECT 'quote_64bit_integers';
SELECT * FROM oracle ORDER BY id SETTINGS output_format_json_quote_64bit_integers = 1, output_format_json_escape_forward_slashes = 0;

SELECT 'tuple of paths';
SELECT id, JSON_VALUE(j, ('$.a.b', '$.ao[*].k')) = JSON_VALUE(j::String, ('$.a.b', '$.ao[*].k')),
    JSON_QUERY(j, ('$.a', ('$.p', '$.t'))) = JSON_QUERY(j::String, ('$.a', ('$.p', '$.t')))
FROM t ORDER BY id;

SELECT 'MergeTree';
CREATE TABLE t_merge_tree ENGINE = MergeTree ORDER BY id AS SELECT * FROM t;
RENAME TABLE t TO t_memory;
RENAME TABLE t_merge_tree TO t;
SELECT * FROM oracle ORDER BY id;

SELECT 'escaped dots';
DROP TABLE t;
CREATE TABLE t (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t SETTINGS json_type_escape_dots_in_keys = 1 VALUES (1, '{"a.b":1,"a":{"c":2}}'), (2, '{"a%2Eb":3}');
WITH ['$["a.b"]', '$."a%2Eb"', '$.a', '$.a.c', '$'] AS P
SELECT id, JSON_QUERY(j, P), arrayFilter((p, x, y) -> x != y, P, JSON_QUERY(j, P), JSON_QUERY(j::String, P))
FROM t ORDER BY id SETTINGS json_type_escape_dots_in_keys = 1;
WITH ['$["a.b"]', '$."a%2Eb"', '$.a', '$.a.c', '$'] AS P
SELECT id, JSON_QUERY(j, P), arrayFilter((p, x, y) -> x != y, P, JSON_QUERY(j, P), JSON_QUERY(j::String, P))
FROM t ORDER BY id SETTINGS json_type_escape_dots_in_keys = 0;

SELECT 'Nullable, Dynamic';
SELECT JSON_VALUE(x, '$.a') <=> JSON_VALUE(x::Nullable(String), '$.a'), JSON_QUERY(x, '$') <=> JSON_QUERY(x::Nullable(String), '$')
FROM (SELECT arrayJoin([NULL, '{"a":1}'])::Nullable(JSON) AS x);
SELECT JSON_VALUE(d, '$.a.b'), JSON_QUERY(d, '$.a'), JSON_QUERY(d, '$') = JSON_QUERY(d::String, '$')
FROM (SELECT '{"a":{"b":1}}'::JSON::Dynamic AS d);

SELECT 'allow_simdjson';
SELECT JSON_VALUE(j, '$.a') FROM t SETTINGS allow_simdjson = 0; -- { serverError NOT_IMPLEMENTED }
SELECT JSON_VALUE(j::String, '$.a') FROM t SETTINGS allow_simdjson = 0; -- { serverError NOT_IMPLEMENTED }

SELECT 'many rows';
WITH ['$.id', '$.k3', '$.o', '$.o.v1', '$.o.v1[1].w', '$.o.v2[*]'] AS P
SELECT count(), countIf(JSON_QUERY(j, P) = JSON_QUERY(j::String, P) AND JSON_VALUE(j, P) = JSON_VALUE(j::String, P)),
    countIf(JSON_QUERY(j, '$.o.v1[1].w') != '')
FROM
(
    SELECT ('{"id":' || toString(number) || ',"k' || toString(number % 7) || '":' || toString(number) || ',"o":{"v' || toString(number % 3)
        || '":[' || toString(number) || ',{"w":' || toString(number % 5) || '}]}}')::JSON(max_dynamic_paths = 4) AS j
    FROM numbers(1000)
);

SELECT 'deep key';
CREATE TABLE t_deep (j JSON) ENGINE = Memory;
INSERT INTO t_deep SELECT ('{"' || arrayStringConcat(arrayMap(x -> 'k', range(1100)), '.') || '":1,"o":2}')::JSON;
SELECT JSON_VALUE(j, '$.o'), JSON_VALUE(j::String, '$.o'), JSON_EXISTS(j, '$.o'), JSON_EXISTS(j::String, '$.o') FROM t_deep;
SELECT JSON_QUERY(j, '$') FROM t_deep; -- { serverError INCORRECT_DATA }
DROP TABLE t_deep;

DROP VIEW oracle;
DROP TABLE t;
DROP TABLE t_memory;
