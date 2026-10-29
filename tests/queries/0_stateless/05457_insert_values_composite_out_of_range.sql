-- A value of a composite type in `INSERT ... VALUES` with an integer that does not fit its element type must be rejected,
-- not stored as an empty value, the default or NULL.

DROP TABLE IF EXISTS t_values_composite;
CREATE TABLE t_values_composite (id UInt8, m Map(String, UInt64)) ENGINE = Memory;

SET input_format_values_interpret_expressions = 1;
SET input_format_null_as_default = 1;

INSERT INTO t_values_composite VALUES (1, '{''a'':1,''b'':2}');
INSERT INTO t_values_composite VALUES (2, '{''a'':1,''b'':-1}'); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite VALUES (3, '{''a'':1,''b'':18446744073709551616}'); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite VALUES (5, map('a', 1, 'b', -1)); -- { error ARGUMENT_OUT_OF_BOUND }
-- A NULL value is still replaced with the default.
INSERT INTO t_values_composite VALUES (4, NULL);

SELECT id, m FROM t_values_composite ORDER BY id;
DROP TABLE t_values_composite;

-- Without templates, every value goes through the same conversion.
DROP TABLE IF EXISTS t_values_composite_no_templates;
CREATE TABLE t_values_composite_no_templates (id UInt8, a Array(UInt8), t Tuple(UInt8, String), nt Nullable(Tuple(UInt8, String))) ENGINE = Memory;

SET input_format_values_deduce_templates_of_expressions = 0;

INSERT INTO t_values_composite_no_templates VALUES (1, '[1]', '(1,''x'')', '(1,''x'')');
INSERT INTO t_values_composite_no_templates VALUES (2, '[300]', '(1,''x'')', '(1,''x'')'); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_no_templates VALUES (3, '[1]', '(300,''x'')', '(1,''x'')'); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_no_templates VALUES (4, '[1]', '(1,''x'')', '(300,''x'')'); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_no_templates VALUES (5, [299 + 1], (1, 'x'), (1, 'x')); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_no_templates VALUES (6, '[1]', '(1,''x'')', NULL);

SELECT * FROM t_values_composite_no_templates ORDER BY id;
DROP TABLE t_values_composite_no_templates;
