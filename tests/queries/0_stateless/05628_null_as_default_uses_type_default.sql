-- An explicit null read with `input_format_null_as_default` must store the default of the column type, not the zero of the column.
SELECT 'JSONEachRow', * FROM format(JSONEachRow, 'e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4), t Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2))', '{"e": null, "m": null, "t": null}');
SELECT 'JSONStringsEachRow', * FROM format(JSONStringsEachRow, 'e Enum8(\'a\' = 1, \'b\' = 2)', '{"e": "NULL"}');
SELECT 'CSV', * FROM format(CSV, 'e Enum8(\'a\' = 1, \'b\' = 2)', '\\N\n');
SELECT 'CSV at EOF', * FROM format(CSV, 'e Enum8(\'a\' = 1, \'b\' = 2)', '\\N');
SELECT 'TSV', * FROM format(TSV, 'e Enum8(\'a\' = 1, \'b\' = 2)', '\\N\n');
SELECT 'TSV at EOF', * FROM format(TSV, 'e Enum8(\'a\' = 1, \'b\' = 2)', '\\N');
SELECT 'TSVRaw', * FROM format(TSVRaw, 'e Enum8(\'a\' = 1, \'b\' = 2)', '\\N\n');
SELECT 'TSKV', * FROM format(TSKV, 'e Enum8(\'a\' = 1, \'b\' = 2)', 'e=\\N\n');
SELECT 'CustomSeparated', * FROM format(CustomSeparated, 'e Enum8(\'a\' = 1, \'b\' = 2)', 'NULL\n') SETTINGS format_custom_escaping_rule = 'Quoted';
SELECT 'MySQLDump', * FROM format(MySQLDump, 'e Enum8(\'a\' = 1, \'b\' = 2)', 'INSERT INTO `t` VALUES (NULL);\n');
SELECT 'Values', * FROM format(Values, 't Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2))', '(NULL)');
SELECT 'BSONEachRow', * FROM format(BSONEachRow, 'e Enum8(\'a\' = 1, \'b\' = 2), a Array(Enum8(\'a\' = 1, \'b\' = 2))', x'130000000a6500046100080000000a30000000');
SELECT 'Nothing', toTypeName(x), count() FROM format(JSONEachRow, 'x Nothing', '{"x": null}') GROUP BY 2;

DROP TABLE IF EXISTS t_null_as_default_enum;
CREATE TABLE t_null_as_default_enum (id UInt8, e Enum8('a' = 1, 'b' = 2), d Enum8('a' = 1, 'b' = 2) DEFAULT 'b') ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null_as_default_enum FORMAT JSONEachRow {"id": 1, "e": null, "d": null}

INSERT INTO t_null_as_default_enum SETTINGS input_format_defaults_for_omitted_fields = 0 FORMAT JSONEachRow {"id": 2, "e": null, "d": null}

SELECT * FROM t_null_as_default_enum ORDER BY id;
DROP TABLE t_null_as_default_enum;
