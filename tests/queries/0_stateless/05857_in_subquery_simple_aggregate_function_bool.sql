-- IN over a Bool or SimpleAggregateFunction(f, Bool) subquery set matches every nonzero value, with or without the primary key (#132493).

DROP TABLE IF EXISTS t_u16;
DROP TABLE IF EXISTS t_u8;
DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_tuple;

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

SELECT 'probe';
SELECT materialize(toUInt8(2)) IN (SELECT anySimpleState(true)), materialize(toUInt16(2)) IN (SELECT anySimpleState(true));
SELECT materialize(toNullable(toUInt8(2))) IN (SELECT anySimpleState(true)), materialize(toUInt8(0)) IN (SELECT anySimpleState(true));
SELECT materialize(toUInt8(2)) IN (SELECT anySimpleState(true)) SETTINGS rewrite_in_to_join = 1;

SELECT 'primary key';
SELECT groupArray(b) FROM (SELECT b FROM t_bool WHERE b IN (SELECT anySimpleState(true)) ORDER BY b) SETTINGS force_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_u16 WHERE w IN (SELECT toUInt16(2)) ORDER BY w) SETTINGS force_primary_key = 1;

DROP TABLE t_u16;
DROP TABLE t_u8;
DROP TABLE t_bool;
DROP TABLE t_array;
DROP TABLE t_tuple;
