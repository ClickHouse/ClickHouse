-- `input_format_tsv_enum_as_number` is an input format setting: it must not make a comparison read an enum name as a number.

SET input_format_tsv_enum_as_number = 1;

DROP TABLE IF EXISTS t_enum_cmp;
CREATE TABLE t_enum_cmp (level Enum8('Error' = 3, 'Warning' = 4, 'Information' = 6, 'Debug' = 7)) ENGINE = Memory;
INSERT INTO t_enum_cmp VALUES ('Error'), ('Information'), ('Debug');

SELECT groupArray(level) FROM t_enum_cmp WHERE level <= 'Information';
SELECT groupArray(level) FROM t_enum_cmp WHERE level = 'Debug';
SELECT groupArray(level) FROM t_enum_cmp WHERE level != 'Error' SETTINGS validate_enum_literals_in_operators = 1;
SELECT groupArray(level) FROM t_enum_cmp WHERE level > 4;

DROP TABLE t_enum_cmp;
