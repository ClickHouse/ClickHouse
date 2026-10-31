-- Regression tests for `optimize_push_subcolumns_into_subqueries`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_json_object;
DROP TABLE IF EXISTS t_push_subcolumns_alias_merge;
DROP TABLE IF EXISTS t_push_subcolumns_enum_key;
DROP TABLE IF EXISTS t_push_subcolumns_fixed_string_key;
DROP TABLE IF EXISTS t_push_subcolumns_flat_name;
DROP TABLE IF EXISTS t_push_subcolumns_json_type;
DROP TABLE IF EXISTS t_push_subcolumns_target_use;

-- A field of a JSON array element can be an object rather than a literal.
SELECT 'json object field';
CREATE TABLE t_push_subcolumns_json_object (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_json_object VALUES ('{"a":[{"b":1,"c":{"e":10}}]}');
SELECT isNull(x.a[1].c), toJSONString(x.a[1].c) FROM (SELECT json AS x FROM t_push_subcolumns_json_object);
SELECT isNull(x.a[1].c), toJSONString(x.a[1].c) FROM (SELECT json AS x FROM t_push_subcolumns_json_object) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT x.a[1].b, x.a[1].c.e FROM (SELECT json AS x FROM t_push_subcolumns_json_object);

-- The alias list of a subquery is pruned together with its projection, which `StorageMerge` rebuilds.
SELECT 'alias list with merge';
CREATE TABLE t_push_subcolumns_alias_merge (tup Tuple(a UInt32, b UInt32)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_alias_merge VALUES ((1, 2));
SELECT x.a FROM (SELECT t.tup FROM merge(currentDatabase(), '^t_push_subcolumns_alias_merge$') AS t CROSS JOIN numbers(1) AS n) AS s(x);

-- A numeric key that is not a member of the Enum.
SELECT 'enum key';
CREATE TABLE t_push_subcolumns_enum_key (m Map(Enum8('a' = 1), UInt32)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_enum_key VALUES ({'a': 42});
SELECT m[-1], m[1], m['a'] FROM (SELECT m FROM t_push_subcolumns_enum_key);

-- A string key shorter than the `FixedString` map key does not match the padded key.
SELECT 'fixed string key';
CREATE TABLE t_push_subcolumns_fixed_string_key (m Map(FixedString(2), UInt32)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_fixed_string_key SELECT map(toFixedString('a', 2), toUInt32(42));
SELECT x['a'], x[toFixedString('a', 2)] FROM (SELECT m AS x FROM t_push_subcolumns_fixed_string_key);
SELECT x['a'], x[toFixedString('a', 2)] FROM (SELECT m AS x FROM t_push_subcolumns_fixed_string_key) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- Reading `b` of the subcolumn `a` is not reading the flat subcolumn `a.b`.
SELECT 'flat name';
CREATE TABLE t_push_subcolumns_flat_name (c Tuple(`a.b` UInt32, a Tuple(b UInt32))) ENGINE = Memory;
INSERT INTO t_push_subcolumns_flat_name VALUES ((99, (3)));
SELECT tupleElement(x, 'b') FROM (SELECT tupleElement(c, 'a') AS x FROM (SELECT c FROM t_push_subcolumns_flat_name));
SELECT getSubcolumn(x, 'b') FROM (SELECT getSubcolumn(c, 'a') AS x FROM (SELECT c FROM t_push_subcolumns_flat_name));

-- A type of the `Dynamic` value of a JSON path is not another JSON path.
SELECT 'json path type';
CREATE TABLE t_push_subcolumns_json_type (j JSON(`a.Int64` Nullable(Int64))) ENGINE = Memory;
INSERT INTO t_push_subcolumns_json_type VALUES ('{"a":7,"a.Int64":99}');
SELECT getSubcolumn(x, 'Int64') FROM (SELECT getSubcolumn(j, 'a') AS x FROM (SELECT j FROM t_push_subcolumns_json_type));
SELECT getSubcolumn(x, 'Int64') FROM (SELECT getSubcolumn(j, 'a') AS x FROM (SELECT j FROM t_push_subcolumns_json_type)) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- The whole column is still read for the filter of the subquery, so the subcolumn is extracted from it.
SELECT 'whole column used in the subquery';
CREATE TABLE t_push_subcolumns_target_use (tup Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_target_use SELECT (toString(number), number % 255) FROM numbers(10);
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_push_subcolumns_target_use WHERE cityHash64(tup) > 0)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.a%';
SELECT sum(cityHash64(tup.a)) = (SELECT sum(cityHash64(toString(number))) FROM numbers(10))
FROM (SELECT tup FROM t_push_subcolumns_target_use WHERE cityHash64(tup) > 0);

DROP TABLE t_push_subcolumns_json_object;
DROP TABLE t_push_subcolumns_alias_merge;
DROP TABLE t_push_subcolumns_enum_key;
DROP TABLE t_push_subcolumns_fixed_string_key;
DROP TABLE t_push_subcolumns_flat_name;
DROP TABLE t_push_subcolumns_json_type;
DROP TABLE t_push_subcolumns_target_use;
