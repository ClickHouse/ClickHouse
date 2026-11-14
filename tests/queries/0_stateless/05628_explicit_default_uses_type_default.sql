-- An explicit request for the column default must store the default of the type, not the zero of the column.
DROP TABLE IF EXISTS t_explicit_default;
CREATE TABLE t_explicit_default (id UInt8, e Enum8('a' = 1, 'b' = 2), m Enum8('m' = -3, 'n' = 0, 'o' = 4), t Tuple(n UInt8, e Enum8('a' = 1, 'b' = 2)), d Enum8('a' = 1, 'b' = 2) DEFAULT 'b', j JSON(e Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_explicit_default VALUES (1, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT);
INSERT INTO t_explicit_default SELECT * FROM format(RowBinaryWithDefaults, 'id UInt8, e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4), t Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2)), d Enum8(\'a\' = 1, \'b\' = 2) DEFAULT \'b\', j JSON(e Enum8(\'a\' = 1, \'b\' = 2))', x'00020101010101');
INSERT INTO t_explicit_default SETTINGS input_format_json_empty_as_default = 1 FORMAT JSONEachRow {"id": 3, "e": "", "m": "", "t": "", "d": "", "j": ""}

SELECT * FROM t_explicit_default ORDER BY id;
SELECT * FROM format(JSONStringsEachRow, 'e Enum8(\'a\' = 1, \'b\' = 2)', '{"e": ""}') SETTINGS input_format_json_empty_as_default = 1;
SELECT * FROM format(RowBinaryWithNamesAndTypesAndDefaults, 'e Enum8(\'a\' = 1, \'b\' = 2)', concat(unhex('01016517'), 'Enum8(\'a\' = 1, \'b\' = 2)', unhex('01')));
SELECT toTypeName(x), count() FROM format(Values, 'x Nothing', '(DEFAULT)') GROUP BY 1;
DROP TABLE t_explicit_default;
