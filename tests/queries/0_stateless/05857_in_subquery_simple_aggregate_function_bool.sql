SET use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t_saf_bool_in_u16;
DROP TABLE IF EXISTS t_saf_bool_in_u8;
DROP TABLE IF EXISTS t_saf_bool_in_bool;

CREATE TABLE t_saf_bool_in_u16 (w UInt16) ENGINE = MergeTree ORDER BY w SETTINGS index_granularity = 1;
CREATE TABLE t_saf_bool_in_u8 (w UInt8) ENGINE = MergeTree ORDER BY w SETTINGS index_granularity = 1;
CREATE TABLE t_saf_bool_in_bool (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
INSERT INTO t_saf_bool_in_u16 VALUES (0), (1), (2), (3);
INSERT INTO t_saf_bool_in_u8 VALUES (0), (1), (2), (3);
INSERT INTO t_saf_bool_in_bool VALUES (false), (true);

SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u16 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u16 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 0;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u8 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u8 WHERE w IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 0;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u16 WHERE w NOT IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u8 WHERE w NOT IN (SELECT anySimpleState(true)) ORDER BY w) SETTINGS use_primary_key = 1;

SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u16 WHERE w IN (SELECT true) ORDER BY w) SETTINGS use_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u8 WHERE w IN (SELECT true) ORDER BY w) SETTINGS use_primary_key = 1;

SELECT materialize(toUInt8(2)) IN (SELECT anySimpleState(true)), materialize(toUInt16(2)) IN (SELECT anySimpleState(true));
SELECT materialize(toNullable(toUInt8(2))) IN (SELECT anySimpleState(true)), materialize(toUInt8(0)) IN (SELECT anySimpleState(true));

-- The primary key is still used for a Bool key and for a set that is not Bool.
SELECT groupArray(b) FROM (SELECT b FROM t_saf_bool_in_bool WHERE b IN (SELECT anySimpleState(true)) ORDER BY b) SETTINGS force_primary_key = 1;
SELECT groupArray(w) FROM (SELECT w FROM t_saf_bool_in_u16 WHERE w IN (SELECT toUInt16(2)) ORDER BY w) SETTINGS force_primary_key = 1;

DROP TABLE t_saf_bool_in_u16;
DROP TABLE t_saf_bool_in_u8;
DROP TABLE t_saf_bool_in_bool;
