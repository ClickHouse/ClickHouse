SET enable_json_type = 1;

DROP TABLE IF EXISTS t_eph;
DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_chain;
DROP TABLE IF EXISTS t_nested_tuple;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_nested;

-- The parent of the subcolumn is EPHEMERAL: it is computed only because the defaults need it.
CREATE TABLE t_eph
(
    line String,
    j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int64, b Int64)'),
    x Int64 MATERIALIZED j.a,
    s Int64 DEFAULT j.a + j.b
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_eph (line) VALUES ('{"a":1,"b":2}');
INSERT INTO t_eph (line) SELECT '{"a":3,"b":4}';
SELECT x, s FROM t_eph ORDER BY x;

-- The parent referenced both plainly and through subcolumns is computed once (this worked before the fix).
CREATE TABLE t_mixed
(
    line String,
    j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int64, b Int64)'),
    x Int64 MATERIALIZED j.a + j.b,
    y Int64 MATERIALIZED tupleElement(j, 'b')
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mixed (line) VALUES ('{"a":1,"b":2}');
SELECT x, y FROM t_mixed;

-- The parent is an ALIAS column.
CREATE TABLE t_alias
(
    line String,
    j Tuple(a Int64, b Int64) ALIAS JSONExtract(line, 'Tuple(a Int64, b Int64)'),
    x Int64 MATERIALIZED j.a
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias (line) VALUES ('{"a":1,"b":2}');
SELECT x, j.a FROM t_alias;

-- The parent is an ALIAS of an EPHEMERAL column.
CREATE TABLE t_chain
(
    line String,
    j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int64, b Int64)'),
    k Tuple(a Int64, b Int64) ALIAS j,
    x Int64 MATERIALIZED k.a
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain (line) VALUES ('{"a":7,"b":2}');
SELECT x FROM t_chain;

-- Every kind of subcolumn, and the Memory engine.
CREATE TABLE t_nested_tuple (line String, j Tuple(a Tuple(c Int64), b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Tuple(c Int64), b Int64)'), x Int64 MATERIALIZED j.a.c) ENGINE = Memory;
INSERT INTO t_nested_tuple (line) VALUES ('{"a":{"c":5},"b":2}');
SELECT x FROM t_nested_tuple;

CREATE TABLE t_map (line String, m Map(String, Int64) EPHEMERAL JSONExtract(line, 'Map(String, Int64)'), x Array(String) MATERIALIZED m.keys) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map (line) VALUES ('{"a":1,"b":2}');
SELECT x FROM t_map;

CREATE TABLE t_nullable (line String, n Nullable(Int64) EPHEMERAL JSONExtract(line, 'Nullable(Int64)'), x UInt8 MATERIALIZED n.null) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable (line) VALUES ('1');
SELECT x FROM t_nullable;

CREATE TABLE t_array (line String, a Array(Int64) EPHEMERAL JSONExtract(line, 'Array(Int64)'), x UInt64 MATERIALIZED a.size0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_array (line) VALUES ('[1,2,3]');
SELECT x FROM t_array;

CREATE TABLE t_json (line String, js JSON EPHEMERAL line::JSON, x Int64 MATERIALIZED js.a::Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json (line) VALUES ('{"a":1,"b":2}');
SELECT x FROM t_json;

-- A Nested member is a real column, not a subcolumn: unchanged behaviour, inserted or omitted.
CREATE TABLE t_nested (line String, n Nested(a Int64, b Int64), x Array(Int64) MATERIALIZED n.a) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested (line, n.a, n.b) VALUES ('{}', [1], [2]);
INSERT INTO t_nested (line) VALUES ('{}');
SELECT x FROM t_nested ORDER BY x;

DROP TABLE t_eph;
DROP TABLE t_mixed;
DROP TABLE t_alias;
DROP TABLE t_chain;
DROP TABLE t_nested_tuple;
DROP TABLE t_map;
DROP TABLE t_nullable;
DROP TABLE t_array;
DROP TABLE t_json;
DROP TABLE t_nested;
