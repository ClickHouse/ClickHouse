-- `CHECK d = n AND n = f` makes `d` equal to `f`, but `CAST(f, 'Decimal(38, 18)')` rounds: the substitution must not replace `d` with it

SET convert_query_to_cnf = 1, optimize_append_index = 0;

DROP TABLE IF EXISTS t_cast_from_float;

CREATE TABLE t_cast_from_float (d Decimal128(18), n Int64, f Float64, CONSTRAINT c CHECK d = n AND n = f) ENGINE = MergeTree ORDER BY f SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_cast_from_float VALUES (12345, 12345, 12345);

SELECT 'filter', count() FROM t_cast_from_float WHERE d = toDecimal128(12345, 18) SETTINGS optimize_substitute_columns = 0;
SELECT 'filter', count() FROM t_cast_from_float WHERE d = toDecimal128(12345, 18) SETTINGS optimize_substitute_columns = 1;

SELECT 'projection', d FROM t_cast_from_float WHERE n > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'projection', d FROM t_cast_from_float WHERE n > 0 SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_cast_from_float;
