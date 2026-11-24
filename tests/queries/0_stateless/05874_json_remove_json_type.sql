-- Tags: no-fasttest
-- no-fasttest: JSONRemove requires RapidJSON

-- JSONRemove on a JSON column returns the same JSON type.
-- The result must be the same as for the column cast to String, with the result cast back to the type.

DROP TABLE IF EXISTS t_remove;
CREATE TABLE t_remove (id UInt32, j JSON(max_dynamic_paths = 3, `a.b` UInt32, c Nullable(String))) ENGINE = Memory;
INSERT INTO t_remove VALUES
    (1, '{"a":{"b":7,"x":1},"a-c":"s","d":[1,null,3],"e":[{"f":1,"g":[{"h":"q"}]},{"f":2}],"z":{"y":[[1,2],[3]]},"k1":1,"k2":2.5,"k3":true}'),
    (2, '{"c":"str","k4":"2020-01-01","k5":"2020-01-01 00:00:00","n":null}'),
    (3, '{}'),
    (4, '{"a":{"x":"y"},"d":"not an array","e":[{"f":[{"g":1},2]}],"k6":{"m":[{"n":1,"o":2}]},"k7":[1,"x",{"b":1}]}');

SELECT 'typed, dynamic and shared data paths';
SELECT id, JSONDynamicPaths(j), JSONSharedDataPaths(j) FROM t_remove ORDER BY id;

SELECT 'results';
SELECT '$.a', id, JSONRemove(j, '$.a') FROM t_remove ORDER BY id;
SELECT '$.a.b', id, JSONRemove(j, '$.a.b') FROM t_remove ORDER BY id;
SELECT '$.c', id, JSONRemove(j, '$.c') FROM t_remove ORDER BY id;
SELECT '$."a-c"', id, JSONRemove(j, '$."a-c"') FROM t_remove ORDER BY id;
SELECT '$.d[1]', id, JSONRemove(j, '$.d[1]') FROM t_remove ORDER BY id;
SELECT '$.e[0].f', id, JSONRemove(j, '$.e[0].f') FROM t_remove ORDER BY id;
SELECT '$.e[0].g[0].h', id, JSONRemove(j, '$.e[0].g[0].h') FROM t_remove ORDER BY id;
SELECT '$.e[0].f[0].g', id, JSONRemove(j, '$.e[0].f[0].g') FROM t_remove ORDER BY id;
SELECT '$.z.y[0][1]', id, JSONRemove(j, '$.z.y[0][1]') FROM t_remove ORDER BY id;
SELECT '$.k4', id, JSONRemove(j, '$.k4') FROM t_remove ORDER BY id;
SELECT '$.k6.m[0].n', id, JSONRemove(j, '$.k6.m[0].n') FROM t_remove ORDER BY id;
SELECT '$.k7[2].b', id, JSONRemove(j, '$.k7[2].b') FROM t_remove ORDER BY id;
SELECT '$.d[0], $.d[0]', id, JSONRemove(j, '$.d[0]', '$.d[0]') FROM t_remove ORDER BY id;

-- For each path: the number of rows where the result differs from the String input.
SELECT 'compare with String';
SELECT '$.a', countIf(toString(JSONRemove(j, '$.a')) != toString(CAST(JSONRemove(j::String, '$.a'), toTypeName(j)))) FROM t_remove;
SELECT '$.a.b', countIf(toString(JSONRemove(j, '$.a.b')) != toString(CAST(JSONRemove(j::String, '$.a.b'), toTypeName(j)))) FROM t_remove;
SELECT '$.a.x', countIf(toString(JSONRemove(j, '$.a.x')) != toString(CAST(JSONRemove(j::String, '$.a.x'), toTypeName(j)))) FROM t_remove;
SELECT '$.c', countIf(toString(JSONRemove(j, '$.c')) != toString(CAST(JSONRemove(j::String, '$.c'), toTypeName(j)))) FROM t_remove;
SELECT '$."a-c"', countIf(toString(JSONRemove(j, '$."a-c"')) != toString(CAST(JSONRemove(j::String, '$."a-c"'), toTypeName(j)))) FROM t_remove;
SELECT '$.d', countIf(toString(JSONRemove(j, '$.d')) != toString(CAST(JSONRemove(j::String, '$.d'), toTypeName(j)))) FROM t_remove;
SELECT '$.d[1]', countIf(toString(JSONRemove(j, '$.d[1]')) != toString(CAST(JSONRemove(j::String, '$.d[1]'), toTypeName(j)))) FROM t_remove;
SELECT '$.d[5]', countIf(toString(JSONRemove(j, '$.d[5]')) != toString(CAST(JSONRemove(j::String, '$.d[5]'), toTypeName(j)))) FROM t_remove;
SELECT '$.e', countIf(toString(JSONRemove(j, '$.e')) != toString(CAST(JSONRemove(j::String, '$.e'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0]', countIf(toString(JSONRemove(j, '$.e[0]')) != toString(CAST(JSONRemove(j::String, '$.e[0]'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[1].f', countIf(toString(JSONRemove(j, '$.e[1].f')) != toString(CAST(JSONRemove(j::String, '$.e[1].f'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0].f', countIf(toString(JSONRemove(j, '$.e[0].f')) != toString(CAST(JSONRemove(j::String, '$.e[0].f'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0].g[0].h', countIf(toString(JSONRemove(j, '$.e[0].g[0].h')) != toString(CAST(JSONRemove(j::String, '$.e[0].g[0].h'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0].g[0]', countIf(toString(JSONRemove(j, '$.e[0].g[0]')) != toString(CAST(JSONRemove(j::String, '$.e[0].g[0]'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0].f[0].g', countIf(toString(JSONRemove(j, '$.e[0].f[0].g')) != toString(CAST(JSONRemove(j::String, '$.e[0].f[0].g'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[0].f[1]', countIf(toString(JSONRemove(j, '$.e[0].f[1]')) != toString(CAST(JSONRemove(j::String, '$.e[0].f[1]'), toTypeName(j)))) FROM t_remove;
SELECT '$.e[5].f', countIf(toString(JSONRemove(j, '$.e[5].f')) != toString(CAST(JSONRemove(j::String, '$.e[5].f'), toTypeName(j)))) FROM t_remove;
SELECT '$.z', countIf(toString(JSONRemove(j, '$.z')) != toString(CAST(JSONRemove(j::String, '$.z'), toTypeName(j)))) FROM t_remove;
SELECT '$.z.y', countIf(toString(JSONRemove(j, '$.z.y')) != toString(CAST(JSONRemove(j::String, '$.z.y'), toTypeName(j)))) FROM t_remove;
SELECT '$.z.y[0]', countIf(toString(JSONRemove(j, '$.z.y[0]')) != toString(CAST(JSONRemove(j::String, '$.z.y[0]'), toTypeName(j)))) FROM t_remove;
SELECT '$.z.y[0][1]', countIf(toString(JSONRemove(j, '$.z.y[0][1]')) != toString(CAST(JSONRemove(j::String, '$.z.y[0][1]'), toTypeName(j)))) FROM t_remove;
SELECT '$.k4', countIf(toString(JSONRemove(j, '$.k4')) != toString(CAST(JSONRemove(j::String, '$.k4'), toTypeName(j)))) FROM t_remove;
SELECT '$.k5', countIf(toString(JSONRemove(j, '$.k5')) != toString(CAST(JSONRemove(j::String, '$.k5'), toTypeName(j)))) FROM t_remove;
SELECT '$.k6', countIf(toString(JSONRemove(j, '$.k6')) != toString(CAST(JSONRemove(j::String, '$.k6'), toTypeName(j)))) FROM t_remove;
SELECT '$.k6.m[0].n', countIf(toString(JSONRemove(j, '$.k6.m[0].n')) != toString(CAST(JSONRemove(j::String, '$.k6.m[0].n'), toTypeName(j)))) FROM t_remove;
SELECT '$.k7[2].b', countIf(toString(JSONRemove(j, '$.k7[2].b')) != toString(CAST(JSONRemove(j::String, '$.k7[2].b'), toTypeName(j)))) FROM t_remove;
SELECT '$.k7[1]', countIf(toString(JSONRemove(j, '$.k7[1]')) != toString(CAST(JSONRemove(j::String, '$.k7[1]'), toTypeName(j)))) FROM t_remove;
SELECT '$.a[0]', countIf(toString(JSONRemove(j, '$.a[0]')) != toString(CAST(JSONRemove(j::String, '$.a[0]'), toTypeName(j)))) FROM t_remove;
SELECT '$.d.x', countIf(toString(JSONRemove(j, '$.d.x')) != toString(CAST(JSONRemove(j::String, '$.d.x'), toTypeName(j)))) FROM t_remove;
SELECT '$.missing', countIf(toString(JSONRemove(j, '$.missing')) != toString(CAST(JSONRemove(j::String, '$.missing'), toTypeName(j)))) FROM t_remove;
SELECT '$.k1.x', countIf(toString(JSONRemove(j, '$.k1.x')) != toString(CAST(JSONRemove(j::String, '$.k1.x'), toTypeName(j)))) FROM t_remove;
SELECT '$.e.f', countIf(toString(JSONRemove(j, '$.e.f')) != toString(CAST(JSONRemove(j::String, '$.e.f'), toTypeName(j)))) FROM t_remove;

SELECT 'several paths';
SELECT countIf(toString(JSONRemove(j, '$.d[0]', '$.d[0]', '$.e[0].f', '$.a')) != toString(CAST(JSONRemove(j::String, '$.d[0]', '$.d[0]', '$.e[0].f', '$.a'), toTypeName(j)))) FROM t_remove;

SELECT 'result type';
SELECT toTypeName(JSONRemove(j, '$.a')) = toTypeName(j) FROM t_remove LIMIT 1;

SELECT 'values of different types in the shared variant of Dynamic';
DROP TABLE IF EXISTS t_remove_variant;
CREATE TABLE t_remove_variant (id UInt32, j JSON(max_dynamic_types = 1)) ENGINE = Memory;
INSERT INTO t_remove_variant VALUES (1, '{"p": 1}'), (2, '{"p": [1, 2]}'), (3, '{"p": [{"q": 1, "r": [true, 2.5]}]}'), (4, '{"p": "s"}');
SELECT id, dynamicType(j.p), isDynamicElementInSharedData(j.p), JSONRemove(j, '$.p[0]'), JSONRemove(j, '$.p[0].r[1]') FROM t_remove_variant ORDER BY id;
SELECT '$.p', countIf(toString(JSONRemove(j, '$.p')) != toString(CAST(JSONRemove(j::String, '$.p'), toTypeName(j)))) FROM t_remove_variant;
SELECT '$.p[0]', countIf(toString(JSONRemove(j, '$.p[0]')) != toString(CAST(JSONRemove(j::String, '$.p[0]'), toTypeName(j)))) FROM t_remove_variant;
SELECT '$.p[1]', countIf(toString(JSONRemove(j, '$.p[1]')) != toString(CAST(JSONRemove(j::String, '$.p[1]'), toTypeName(j)))) FROM t_remove_variant;
SELECT '$.p[0].q', countIf(toString(JSONRemove(j, '$.p[0].q')) != toString(CAST(JSONRemove(j::String, '$.p[0].q'), toTypeName(j)))) FROM t_remove_variant;
SELECT '$.p[0].r[1]', countIf(toString(JSONRemove(j, '$.p[0].r[1]')) != toString(CAST(JSONRemove(j::String, '$.p[0].r[1]'), toTypeName(j)))) FROM t_remove_variant;

SELECT 'Nullable and constant arguments';
SELECT JSONRemove(j, '$.a.b'), toTypeName(JSONRemove(j, '$.a.b')) FROM (SELECT CAST(x, 'Nullable(JSON)') AS j FROM values('x Nullable(String)', ('{"a":{"b":1,"c":2}}'), (NULL)));
SELECT JSONRemove('{"a":1,"b":[1,2]}'::JSON, '$.b[0]'), toTypeName(JSONRemove('{"a":1,"b":[1,2]}'::JSON, '$.b[0]'));

SELECT 'escaped dots in keys';
SELECT JSONRemove(j, '$["a.b"]'), CAST(JSONRemove(j::String, '$["a.b"]'), toTypeName(j)), JSONRemove(j, '$.a.b')
FROM (SELECT '{"a.b":1,"a":{"b":2}}'::JSON AS j)
SETTINGS json_type_escape_dots_in_keys = 1;

SELECT 'INSERT SELECT and ALTER UPDATE';
DROP TABLE IF EXISTS t_remove_mt;
CREATE TABLE t_remove_mt (id UInt32, j JSON(max_dynamic_paths = 3, `a.b` UInt32, c Nullable(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_remove_mt SELECT id, JSONRemove(j, '$.d[0]', '$.e[0].f') FROM t_remove;
SELECT id, j FROM t_remove_mt ORDER BY id;
ALTER TABLE t_remove_mt UPDATE j = JSONRemove(j, '$.a', '$.k6.m[0].o') WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, j FROM t_remove_mt ORDER BY id;

SELECT 'errors';
SELECT JSONRemove(j, materialize('$.a')) FROM t_remove; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONRemove(j, '$') FROM t_remove; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove(j, '$[*]') FROM t_remove; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove(1, '$.a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_remove;
DROP TABLE t_remove_variant;
DROP TABLE t_remove_mt;
