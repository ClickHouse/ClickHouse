-- two different `Enum` types are equal by the value, but `CAST` from one to the other reads the name

SET convert_query_to_cnf = 1, optimize_using_constraints = 1;

DROP TABLE IF EXISTS t_enums;
DROP TABLE IF EXISTS t_enum_tuples;
DROP TABLE IF EXISTS t_enum_arrays;
DROP TABLE IF EXISTS t_enum_number;

-- 'a' of `e` and 'b' of `f` are both the value 1
CREATE TABLE t_enums (e Enum8('a' = 1, 'b' = 2), f Enum8('a' = 2, 'b' = 1), CONSTRAINT c CHECK e = f) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enums VALUES ('a', 'b');
SELECT 'enums', e FROM t_enums WHERE f > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'enums', e FROM t_enums WHERE f > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_enum_tuples (e Tuple(Enum8('a' = 1, 'b' = 2)), f Tuple(Enum8('a' = 2, 'b' = 1)), CONSTRAINT c CHECK e = f) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_tuples VALUES (tuple('a'), tuple('b'));
SELECT 'enum tuples', e FROM t_enum_tuples WHERE f.1 > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'enum tuples', e FROM t_enum_tuples WHERE f.1 > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_enum_arrays (e Array(Enum8('a' = 1, 'b' = 2)), f Array(Enum8('a' = 2, 'b' = 1)), CONSTRAINT c CHECK e = f) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_arrays VALUES (['a'], ['b']);
SELECT 'enum arrays', e FROM t_enum_arrays WHERE notEmpty(f) SETTINGS optimize_substitute_columns = 0;
SELECT 'enum arrays', e FROM t_enum_arrays WHERE notEmpty(f) SETTINGS optimize_substitute_columns = 1;

-- a number and an `Enum` convert by the value, so they are still substituted
CREATE TABLE t_enum_number (e Enum8('a' = 1, 'b' = 2), n UInt8, CONSTRAINT c CHECK e = n) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_number VALUES ('b', 2);
SELECT 'enum number', e FROM t_enum_number WHERE n > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'enum number', e FROM t_enum_number WHERE n > 0 SETTINGS optimize_substitute_columns = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT e FROM t_enum_number WHERE n > 0 SETTINGS optimize_substitute_columns = 1) WHERE explain LIKE '%_CAST%';

DROP TABLE t_enums;
DROP TABLE t_enum_tuples;
DROP TABLE t_enum_arrays;
DROP TABLE t_enum_number;
