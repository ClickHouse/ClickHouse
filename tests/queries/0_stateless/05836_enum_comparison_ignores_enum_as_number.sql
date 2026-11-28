-- `input_format_tsv_enum_as_number` is a setting of the TSV formats: elsewhere a string is still read as an enum name.

SET input_format_tsv_enum_as_number = 1;

DROP TABLE IF EXISTS t_enum_cmp;
CREATE TABLE t_enum_cmp (level Enum8('Error' = 3, 'Warning' = 4, 'Information' = 6, 'Debug' = 7)) ENGINE = Memory;
INSERT INTO t_enum_cmp VALUES ('Error'), ('Information'), ('Debug');

SELECT groupArray(level) FROM t_enum_cmp WHERE level <= 'Information';
SELECT groupArray(level) FROM t_enum_cmp WHERE level = 'Debug';
SELECT groupArray(level) FROM t_enum_cmp WHERE level != 'Error' SETTINGS validate_enum_literals_in_operators = 1;
SELECT groupArray(level) FROM t_enum_cmp WHERE level > 4;

DROP TABLE t_enum_cmp;

-- Other readers of a whole string as an enum
SELECT * FROM format(Form, 'e Enum8(\'a\' = 1, \'b\' = 2)', 'e=b');
SELECT CAST('b' AS Variant(Enum8('a' = 1, 'b' = 2), Array(UInt8)));

-- `TSVRaw` still reads the value
SELECT * FROM format(TSVRaw, 'e Enum8(\'a\' = 1, \'b\' = 2)', '2');
SELECT * FROM format(TSVRaw, 'e Nullable(Enum8(\'a\' = 1, \'b\' = 2))', '2');
SELECT * FROM format(TSVRaw, 'e Enum8(\'a\' = 1, \'b\' = 2)', '2x'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
SELECT * FROM format(TSVRaw, 'e Enum8(\'a\' = 1, \'b\' = 2)', 'b'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
