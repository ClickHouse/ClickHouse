-- A `UInt8` member converted to a nested `Bool` is normalized to `true` (stored byte 1), as `CAST` does.
-- Printing the `Bool` alone would hide an invalid stored byte, so check the byte and a comparison.

SET input_format_values_deduce_templates_of_expressions = 0;

DROP TABLE IF EXISTS t_values_nested_bool;
CREATE TABLE t_values_nested_bool (v Tuple(Bool, String)) ENGINE = Memory;
INSERT INTO t_values_nested_bool VALUES ((toUInt8(2), toUInt32(1)));
SELECT toUInt8(v.1), v.1 = true FROM t_values_nested_bool;
DROP TABLE t_values_nested_bool;

SELECT toUInt8(x[1].1), x[1].1 = true FROM values('x Array(Tuple(Bool, String))', ([(toUInt8(2), toUInt32(1))]));

SELECT toUInt8(CAST((toUInt8(2), toUInt32(1)), 'Tuple(Bool, String)').1);
