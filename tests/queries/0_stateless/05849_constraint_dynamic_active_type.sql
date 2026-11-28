-- the active type of a `Dynamic` or `Variant` value decides how it compares, so `CHECK v = 1` does not make `v` the number 1

SET convert_query_to_cnf = 1, optimize_using_constraints = 1;

DROP TABLE IF EXISTS t_dynamic;
DROP TABLE IF EXISTS t_dynamic_mt;
DROP TABLE IF EXISTS t_variant;

-- the name '9' is the value 1 of the `Enum`
CREATE TABLE t_dynamic (v Dynamic, CONSTRAINT c CHECK v = 1) ENGINE = Memory;
INSERT INTO t_dynamic SELECT CAST(CAST('9', 'Enum8(\'9\' = 1, \'b\' = 5, \'a\' = 9)') AS Dynamic);
SELECT 'dynamic', count() FROM t_dynamic WHERE v < '9' SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'dynamic', count() FROM t_dynamic WHERE v < '9' SETTINGS optimize_substitute_columns = 0;

CREATE TABLE t_dynamic_mt (k UInt8, v Dynamic, CONSTRAINT c CHECK v = 1) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dynamic_mt SELECT 0, CAST(CAST('9', 'Enum8(\'9\' = 1, \'b\' = 5, \'a\' = 9)') AS Dynamic);
SELECT 'dynamic substitution', dynamicType(v), v FROM t_dynamic_mt WHERE v > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'dynamic substitution', dynamicType(v), v FROM t_dynamic_mt WHERE v > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_variant (v Variant(Enum8('9' = 1, 'b' = 5, 'a' = 9), UInt64), CONSTRAINT c CHECK v = 1) ENGINE = Memory;
INSERT INTO t_variant SELECT CAST(CAST('9', 'Enum8(\'9\' = 1, \'b\' = 5, \'a\' = 9)') AS Variant(Enum8('9' = 1, 'b' = 5, 'a' = 9), UInt64));
SELECT 'variant', count() FROM t_variant WHERE v < '9' SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'variant', count() FROM t_variant WHERE v < '9' SETTINGS optimize_substitute_columns = 0;

DROP TABLE t_dynamic;
DROP TABLE t_dynamic_mt;
DROP TABLE t_variant;
