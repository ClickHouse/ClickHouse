-- A NULL map byte is a predicate, not a value: any non-zero byte means NULL. `if` forwards its raw
-- condition column as the NULL map of the result, so `number % 3` fills it with the bytes 0, 1 and 2,
-- while the nested value is 1 in every row. A row with the byte 2 is NULL just as a row with the byte 1,
-- so `multiIf` must not choose the branch of such a condition.

SET optimize_if_transform_strings_to_enum = 0;
SET compile_expressions = 0;

-- The NULL map bytes of the condition.
SELECT number, c.null FROM (SELECT number, if(toUInt8(number % 3), NULL, materialize(toUInt8(1))) AS c FROM numbers(3)) ORDER BY number;

-- Constant string branches, with a `String` result and with a `LowCardinality(String)` result.
SELECT number, toTypeName(result), multiIf(if(toUInt8(number % 3), NULL, materialize(toUInt8(1))), 'first', number = 100, 'second', 'else') AS result
FROM numbers(3) ORDER BY number
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 0;

SELECT number, toTypeName(result), multiIf(if(toUInt8(number % 3), NULL, materialize(toUInt8(1))), 'first', number = 100, 'second', 'else') AS result
FROM numbers(3) ORDER BY number
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 1;

-- Numeric branches.
SELECT number, multiIf(if(toUInt8(number % 3), NULL, materialize(toUInt8(1))), 1, number = 100, 2, 3) AS result
FROM numbers(3) ORDER BY number;

-- Non-constant string branches.
SELECT number, multiIf(if(toUInt8(number % 3), NULL, materialize(toUInt8(1))), materialize('first'), number = 100, 'second', 'else') AS result
FROM numbers(3) ORDER BY number;
