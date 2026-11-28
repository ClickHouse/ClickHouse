-- A typed path missing from a binary JSON value must get the default of its type, as in JSON text input.
SET input_format_binary_read_json_as_string = 0;

DROP TABLE IF EXISTS t_json_rb_absent;
CREATE TABLE t_json_rb_absent (id UInt8, j JSON(e Enum8('a' = 1, 'b' = 2), t Tuple(n UInt8, e Enum8('a' = 1, 'b' = 2)))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_rb_absent SELECT * FROM format(RowBinary, 'id UInt8, j JSON(e Enum8(\'a\' = 1, \'b\' = 2), t Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2)))', x'01000201016502030201650201740502');
INSERT INTO t_json_rb_absent SELECT * FROM format(JSONEachRow, 'id UInt8, j JSON(e Enum8(\'a\' = 1, \'b\' = 2), t Tuple(n UInt8, e Enum8(\'a\' = 1, \'b\' = 2)))', '{"id": 4, "j": {}}');

SELECT id, toInt8(j.e), tupleElement(j.t, 'n'), toInt8(tupleElement(j.t, 'e')) FROM t_json_rb_absent ORDER BY id;
SELECT * FROM t_json_rb_absent ORDER BY id;

DROP TABLE t_json_rb_absent;

-- A failed default insert must roll the JSON column back, otherwise the outer Tuple over-pops it.
SELECT x FROM format(RowBinary, 'x Tuple(JSON(t Tuple(s FixedString(10000000), e Enum8(\'a\' = 1))))', unhex('00'))
SETTINGS max_memory_usage = 5000000, max_threads = 1, allow_suspicious_fixed_string_types = 1; -- { serverError MEMORY_LIMIT_EXCEEDED }
