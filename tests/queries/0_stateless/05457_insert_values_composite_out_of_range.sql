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

-- A valid NULL in a `Nullable` element does not hide an element that does not fit.
DROP TABLE IF EXISTS t_values_composite_nullable_element;
CREATE TABLE t_values_composite_nullable_element
(
    id UInt8,
    m Map(String, Tuple(Nullable(UInt8), UInt64, String)),
    nt Nullable(Tuple(Nullable(UInt8), UInt64)),
    a Array(Tuple(Nullable(UInt8), UInt64))
) ENGINE = Memory;

SET input_format_null_as_default = 0;

INSERT INTO t_values_composite_nullable_element VALUES (1, map('a', tuple(NULL, 1, 'x')), tuple(NULL, 1), [tuple(NULL, 1)]);
INSERT INTO t_values_composite_nullable_element VALUES (2, map('a', tuple(NULL, -1, 'x')), tuple(1, 1), [tuple(1, 1)]); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_nullable_element VALUES (3, map('a', tuple(1, 1, 'x')), tuple(NULL, -1), [tuple(1, 1)]); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_nullable_element VALUES (4, map('a', tuple(1, 1, 'x')), tuple(1, 1), [tuple(NULL, -1)]); -- { error ARGUMENT_OUT_OF_BOUND }

SELECT * FROM t_values_composite_nullable_element ORDER BY id;
DROP TABLE t_values_composite_nullable_element;

-- An element that does not fit for another reason than an integer overflow is rejected the same way.
DROP TABLE IF EXISTS t_values_composite_date;
CREATE TABLE t_values_composite_date (a Array(Date)) ENGINE = Memory;
INSERT INTO t_values_composite_date VALUES ([toDate32('2300-01-01')]); -- { error ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_values_composite_date;
DROP TABLE t_values_composite_date;

-- An element of a `Nullable` type that does not fit gets NULL inside a value that stays non-NULL, so the value is rejected as well. A NULL
-- of the source in such an element stays.
DROP TABLE IF EXISTS t_values_composite_nested_nullable;
CREATE TABLE t_values_composite_nested_nullable
(
    id UInt8,
    m Map(String, Nullable(Tuple(UInt64, String))),
    a Array(Nullable(Tuple(UInt64, String)))
) ENGINE = Memory;
INSERT INTO t_values_composite_nested_nullable VALUES (1, map('a', NULL), [NULL]);
INSERT INTO t_values_composite_nested_nullable VALUES (2, map('a', tuple(-1, 'kept')), []); -- { error ARGUMENT_OUT_OF_BOUND }
INSERT INTO t_values_composite_nested_nullable SETTINGS input_format_values_deduce_templates_of_expressions = 0
VALUES (3, map(), [tuple(-1, 'kept')]); -- { error ARGUMENT_OUT_OF_BOUND }
SELECT * FROM t_values_composite_nested_nullable ORDER BY id;
DROP TABLE t_values_composite_nested_nullable;
