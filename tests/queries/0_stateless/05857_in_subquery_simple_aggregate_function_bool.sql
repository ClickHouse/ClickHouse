-- IN over a Bool or SimpleAggregateFunction(f, Bool) subquery set matches every nonzero value, with or without the primary key or a bloom filter index (#132493).

DROP TABLE IF EXISTS t_u16;
DROP TABLE IF EXISTS t_u8;
DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_tuple_bool;
DROP TABLE IF EXISTS t_bloom;
DROP TABLE IF EXISTS t_bloom_map;
DROP TABLE IF EXISTS t_bloom_array;

CREATE TABLE t_u16 (w UInt16) ENGINE = MergeTree ORDER BY w SETTINGS index_granularity = 1;
CREATE TABLE t_u8 (w UInt8) ENGINE = MergeTree ORDER BY w SETTINGS index_granularity = 1;
CREATE TABLE t_bool (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
INSERT INTO t_u16 VALUES (0), (1), (2), (3);
INSERT INTO t_u8 VALUES (0), (1), (2), (3);
INSERT INTO t_bool VALUES (false), (true);
CREATE TABLE t_array (a Array(UInt16)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_tuple (t Tuple(UInt16, UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_array VALUES ([1]), ([2]), ([3]);
INSERT INTO t_tuple VALUES ((1, 1)), ((2, 1)), ((3, 1));
CREATE TABLE t_tuple_bool (t Tuple(UInt16, Bool)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_tuple_bool VALUES ((1, true)), ((2, true)), ((3, true));
CREATE TABLE t_bloom (w UInt8, v UInt16, INDEX iw w TYPE bloom_filter(0.001) GRANULARITY 1, INDEX iv v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom VALUES (0, 0), (1, 1), (2, 2), (3, 3);
CREATE TABLE t_bloom_map (m Map(String, UInt16), INDEX im mapValues(m) TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
CREATE TABLE t_bloom_array (a Array(UInt16), INDEX ia a TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_map VALUES ({'k': 0}), ({'k': 1}), ({'k': 2}), ({'k': 3});
INSERT INTO t_bloom_array VALUES ([0]), ([1]), ([2]), ([3]);

SELECT 'saf';
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 0;
SELECT groupArray(w) FROM (SELECT w FROM t_u8 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u8 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 0;
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w NOT IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u8 WHERE w NOT IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;

SELECT 'bool';
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w IN (SELECT true) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u8 WHERE w IN (SELECT true) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(a) FROM (SELECT a FROM t_array WHERE a IN (SELECT [true]) ORDER BY a) SETTINGS use_primary_key = 1;
SELECT groupArray(t) FROM (SELECT t FROM t_tuple WHERE t IN (SELECT tuple(true, toUInt8(1))) ORDER BY t) SETTINGS use_primary_key = 1;
SELECT groupArray(t) FROM (SELECT t FROM t_tuple_bool WHERE t IN (SELECT tuple(true, true)) ORDER BY t) SETTINGS use_primary_key = 1;

SELECT 'probe';
SELECT materialize(toUInt8(2)) IN (SELECT anySimpleState(true)), materialize(toUInt16(2)) IN (SELECT anySimpleState(true));
SELECT materialize(toNullable(toUInt8(2))) IN (SELECT anySimpleState(true)), materialize(toUInt8(0)) IN (SELECT anySimpleState(true));
SELECT materialize(toUInt8(2)) IN (SELECT anySimpleState(true)) SETTINGS rewrite_in_to_join = 1;

SELECT 'bloom filter';
SELECT groupArray(w) FROM (SELECT w FROM t_bloom WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_skip_indexes = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_bloom WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_skip_indexes = 0;
SELECT groupArray(v) FROM (SELECT v FROM t_bloom WHERE v IN (SELECT true) ORDER BY v) SETTINGS use_skip_indexes = 1;
SELECT groupArray(v) FROM (SELECT v FROM t_bloom WHERE v IN (SELECT true) ORDER BY v) SETTINGS use_skip_indexes = 0;
SELECT groupArray(v) FROM (SELECT m['k'] AS v FROM t_bloom_map WHERE m['k'] IN (SELECT true) ORDER BY v) SETTINGS use_skip_indexes = 1;
SELECT groupArray(v) FROM (SELECT m['k'] AS v FROM t_bloom_map WHERE m['k'] IN (SELECT true) ORDER BY v) SETTINGS use_skip_indexes = 0;
SELECT groupArray(x) FROM (SELECT arrayJoin(a) AS x FROM t_bloom_array WHERE x IN (SELECT true) ORDER BY x) SETTINGS use_skip_indexes = 1;
SELECT groupArray(x) FROM (SELECT arrayJoin(a) AS x FROM t_bloom_array WHERE x IN (SELECT true) ORDER BY x) SETTINGS use_skip_indexes = 0;

SELECT 'primary key';
SELECT groupArray(b) FROM (SELECT b FROM t_bool WHERE b IN (SELECT anySimpleState(true)) ORDER BY b) SETTINGS force_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w IN (SELECT toUInt16(2)) ORDER BY w) SETTINGS force_primary_key = 1;

DROP TABLE t_u16;
DROP TABLE t_u8;
DROP TABLE t_bool;
DROP TABLE t_array;
DROP TABLE t_tuple;
DROP TABLE t_tuple_bool;
DROP TABLE t_bloom;
DROP TABLE t_bloom_map;
DROP TABLE t_bloom_array;
