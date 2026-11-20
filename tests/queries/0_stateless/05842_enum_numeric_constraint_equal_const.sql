-- `CHECK e = 1.0` puts `e` and a `Float64` constant in one equality class.
-- The constant has no direct conversion to the `Enum`, so it must not replace `e`, and the query must not throw.

DROP TABLE IF EXISTS t_enum_float_const;
DROP TABLE IF EXISTS t_enum_int128_const;
DROP TABLE IF EXISTS t_enum_float_column;
DROP TABLE IF EXISTS t_enum_int_const;

SET convert_query_to_cnf = 1, optimize_use_implicit_projections = 0, optimize_substitute_columns = 0;

CREATE TABLE t_enum_float_const (e Enum8('a' = 1, 'b' = 2), CONSTRAINT c CHECK e = 1.0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_float_const VALUES ('a');
SELECT 'float', count() FROM t_enum_float_const WHERE e < 'b' SETTINGS optimize_using_constraints = 0;
SELECT 'float', count() FROM t_enum_float_const WHERE e < 'b' SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_int128_const (e Enum8('a' = 1, 'b' = 2), CONSTRAINT c CHECK e = toInt128(1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_int128_const VALUES ('a');
SELECT 'int128', count() FROM t_enum_int128_const WHERE e < 'b' SETTINGS optimize_using_constraints = 0;
SELECT 'int128', count() FROM t_enum_int128_const WHERE e < 'b' SETTINGS optimize_using_constraints = 1;

-- the constant reaches `e` through the `Float64` column `n`
CREATE TABLE t_enum_float_column (e Enum8('a' = 1, 'b' = 2), n Float64, CONSTRAINT c CHECK e = n AND n = 1.0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_float_column VALUES ('a', 1);
SELECT 'float column', count() FROM t_enum_float_column WHERE e < 'b' SETTINGS optimize_using_constraints = 0;
SELECT 'float column', count() FROM t_enum_float_column WHERE e < 'b' SETTINGS optimize_using_constraints = 1;

-- an integer constant has an exact `Enum` value and still replaces `e`
CREATE TABLE t_enum_int_const (e Enum8('a' = 1, 'b' = 2), CONSTRAINT c CHECK e = 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_int_const VALUES ('a');
SELECT 'int', count() FROM t_enum_int_const WHERE e < 'b' SETTINGS optimize_using_constraints = 0;
SELECT 'int', count() FROM t_enum_int_const WHERE e < 'b' SETTINGS optimize_using_constraints = 1;
SELECT 'int replaced', count()
FROM (EXPLAIN QUERY TREE SELECT count() FROM t_enum_int_const WHERE e < 'b' SETTINGS optimize_using_constraints = 1)
WHERE explain LIKE '%CONSTANT%constant_value_type: Enum8%';

DROP TABLE t_enum_float_const;
DROP TABLE t_enum_int128_const;
DROP TABLE t_enum_float_column;
DROP TABLE t_enum_int_const;
