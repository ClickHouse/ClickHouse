-- `CHECK e = n` compares `e` with `n` by value, but `CAST` from `UInt128` or a decimal to an `Enum` is not supported: the substitution must not replace `e` with it

SET convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_append_index = 0;

DROP TABLE IF EXISTS t_enum_wide_int;
DROP TABLE IF EXISTS t_enum_decimal;

CREATE TABLE t_enum_wide_int (e Enum8('a' = 1, 'b' = 2), n UInt128, CONSTRAINT c CHECK e = n) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_enum_wide_int VALUES ('a', 1);

SELECT 'wide int', e FROM t_enum_wide_int WHERE n > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'wide int', e FROM t_enum_wide_int WHERE n > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_enum_decimal (e Enum8('a' = 1, 'b' = 2), n Int64, d Decimal128(18), CONSTRAINT c CHECK e = n AND n = d) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_enum_decimal VALUES ('a', 1, 1);

SELECT 'decimal', e FROM t_enum_decimal WHERE d > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'decimal', e FROM t_enum_decimal WHERE d > 0 SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_enum_wide_int;
DROP TABLE t_enum_decimal;
