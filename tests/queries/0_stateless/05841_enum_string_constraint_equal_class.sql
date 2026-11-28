-- `CHECK e = n AND n = '9'` puts `e` and '9' in one equality class through the number `n`.
-- '9' is the number 9 there, but a cast of '9' to the `Enum` reads the name '9', so it must not replace `e`.

DROP TABLE IF EXISTS t_enum_equal_class;

SET convert_query_to_cnf = 1, optimize_use_implicit_projections = 0, optimize_substitute_columns = 0;

CREATE TABLE t_enum_equal_class (e Enum8('9' = 1, 'b' = 5, 'a' = 9), n UInt8, CONSTRAINT c CHECK e = n AND n = '9') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_equal_class VALUES ('a', 9);

-- 'a' < 'b' is 9 < 5, false
SELECT 'less', count() FROM t_enum_equal_class WHERE e < 'b' SETTINGS optimize_using_constraints = 0;
SELECT 'less', count() FROM t_enum_equal_class WHERE e < 'b' SETTINGS optimize_using_constraints = 1;
SELECT 'equals', count() FROM t_enum_equal_class WHERE e = 'a' SETTINGS optimize_using_constraints = 0;
SELECT 'equals', count() FROM t_enum_equal_class WHERE e = 'a' SETTINGS optimize_using_constraints = 1;
SELECT 'substitute', e FROM t_enum_equal_class WHERE n > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'substitute', e FROM t_enum_equal_class WHERE n > 0 SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_enum_equal_class;
