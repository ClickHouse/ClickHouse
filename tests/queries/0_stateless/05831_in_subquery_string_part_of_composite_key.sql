-- `IN` over a subquery casts the key into the element type of the set, where a `String` part of a `Tuple`, `Array` or `Map`
-- key is parsed, so many spellings match one element. The index renders the element into one spelling, so it must not be used.

SET enable_parallel_replicas = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_tuple_str_key;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_named;
DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_pairs;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_minmax;

SELECT 'whole-tuple key transform';
CREATE TABLE t_tuple_str_key (k Tuple(String, UInt8)) ENGINE = MergeTree ORDER BY toString(k);
SYSTEM STOP MERGES t_tuple_str_key;
INSERT INTO t_tuple_str_key SELECT tuple('2023-10-29 02:30:00', toUInt8(1));
INSERT INTO t_tuple_str_key SELECT tuple('x', toUInt8(1));
SELECT count() FROM t_tuple_str_key WHERE k NOT IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1))
    SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0;
SELECT countIf(k NOT IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1))) FROM t_tuple_str_key;
SELECT count() FROM t_tuple_str_key WHERE k IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1))
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0, use_query_condition_cache = 0;
SELECT countIf(k IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1))) FROM t_tuple_str_key;

SELECT 'tuple key';
CREATE TABLE t_plain (k Tuple(String, UInt8)) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_plain;
INSERT INTO t_plain SELECT tuple('02', toUInt8(1));
INSERT INTO t_plain SELECT tuple('3', toUInt8(1));
SELECT count() FROM t_plain WHERE k IN (SELECT toUInt8(2), toUInt8(1));
SELECT countIf(k IN (SELECT toUInt8(2), toUInt8(1))) FROM t_plain;
SELECT count() FROM t_plain WHERE k NOT IN (SELECT toUInt8(2), toUInt8(1));
SELECT countIf(k NOT IN (SELECT toUInt8(2), toUInt8(1))) FROM t_plain;

SELECT 'named tuple key, cast by name';
CREATE TABLE t_named (k Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_named;
INSERT INTO t_named SELECT tuple('02', toUInt8(1));
INSERT INTO t_named SELECT tuple('3', toUInt8(1));
SELECT count() FROM t_named WHERE k IN (SELECT CAST(('1', toUInt8(2)), 'Tuple(b String, a UInt8)'));
SELECT countIf(k IN (SELECT CAST(('1', toUInt8(2)), 'Tuple(b String, a UInt8)'))) FROM t_named;

SELECT 'array key';
CREATE TABLE t_arr (a Array(String)) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES t_arr;
INSERT INTO t_arr SELECT ['02'];
INSERT INTO t_arr SELECT ['3'];
SELECT count() FROM t_arr WHERE a IN (SELECT [toUInt8(2)]);
SELECT countIf(a IN (SELECT [toUInt8(2)])) FROM t_arr;

SELECT 'map key';
CREATE TABLE t_map (m Map(String, UInt8)) ENGINE = MergeTree ORDER BY m;
SYSTEM STOP MERGES t_map;
INSERT INTO t_map SELECT map('02', toUInt8(1));
INSERT INTO t_map SELECT map('3', toUInt8(1));
SELECT count() FROM t_map WHERE m IN (SELECT map(toUInt8(2), toUInt8(1)));
SELECT countIf(m IN (SELECT map(toUInt8(2), toUInt8(1)))) FROM t_map;

SELECT 'array of pairs key, map element';
CREATE TABLE t_pairs (k Array(Tuple(String, Int64))) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_pairs;
INSERT INTO t_pairs SELECT [('02', 1)];
INSERT INTO t_pairs SELECT [('3', 1)];
SELECT count() FROM t_pairs WHERE k IN (SELECT CAST(map(2, 1), 'Map(UInt8, Int64)'));
SELECT countIf(k IN (SELECT CAST(map(2, 1), 'Map(UInt8, Int64)'))) FROM t_pairs;

SELECT 'partition key';
CREATE TABLE t_part (k Tuple(String, UInt8)) ENGINE = MergeTree PARTITION BY toString(k) ORDER BY tuple();
SYSTEM STOP MERGES t_part;
INSERT INTO t_part SELECT tuple('2023-10-29 02:30:00', toUInt8(1));
INSERT INTO t_part SELECT tuple('x', toUInt8(1));
SELECT count() FROM t_part WHERE k NOT IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1));
SELECT countIf(k NOT IN (SELECT toDateTime(1698543000, 'Europe/Berlin'), toUInt8(1))) FROM t_part;

SELECT 'minmax skip index';
CREATE TABLE t_minmax (k Tuple(String, UInt8), INDEX i_mm k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_minmax;
INSERT INTO t_minmax SELECT tuple('02', toUInt8(1));
INSERT INTO t_minmax SELECT tuple('3', toUInt8(1));
SELECT count() FROM t_minmax WHERE k IN (SELECT toUInt8(2), toUInt8(1));
SELECT countIf(k IN (SELECT toUInt8(2), toUInt8(1))) FROM t_minmax;

SELECT 'a String element keeps the index';
SELECT extract(explain, '(?:Condition|Parts): .*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_tuple_str_key WHERE k NOT IN (SELECT 'x', toUInt8(1))
) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Parts:%';
SELECT extract(explain, '(?:Condition|Parts): .*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_plain WHERE k IN (SELECT '02', toUInt8(1))
) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Parts:%';

DROP TABLE t_tuple_str_key;
DROP TABLE t_plain;
DROP TABLE t_named;
DROP TABLE t_arr;
DROP TABLE t_map;
DROP TABLE t_pairs;
DROP TABLE t_part;
DROP TABLE t_minmax;
