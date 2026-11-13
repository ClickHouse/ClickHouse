-- Tags: no-fasttest
-- A NULL read from the SQLite format into a non-Nullable column with input_format_null_as_default must store the default of the column type, not 0.

INSERT INTO FUNCTION file(currentDatabase() || '_sqlite_null_as_default_enum.sqlite', SQLite)
SELECT CAST(NULL, 'Nullable(Int8)') AS e, CAST(NULL, 'Nullable(Int8)') AS m, CAST(NULL, 'Nullable(Int8)') AS t, CAST(NULL, 'Nullable(Int8)') AS n
SETTINGS engine_file_truncate_on_insert = 1;

SELECT e, m, t, n
FROM file(currentDatabase() || '_sqlite_null_as_default_enum.sqlite', SQLite, 'e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4), t Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2)), n Nullable(Enum8(\'a\' = 1, \'b\' = 2))')
SETTINGS input_format_null_as_default = 1;

DROP TABLE IF EXISTS t_sqlite_null_as_default_enum;
CREATE TABLE t_sqlite_null_as_default_enum (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sqlite_null_as_default_enum SELECT e FROM file(currentDatabase() || '_sqlite_null_as_default_enum.sqlite', SQLite, 'e Enum8(\'a\' = 1, \'b\' = 2)') SETTINGS input_format_null_as_default = 1;
SELECT e, toInt8(e) FROM t_sqlite_null_as_default_enum;
DROP TABLE t_sqlite_null_as_default_enum;
