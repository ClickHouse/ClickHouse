-- A lazily executed argument of an `Enum` type without the value 0 can reduce to a constant column.
-- Its unused rows must be left as is instead of being treated as a full column.

SELECT 'coalesce';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'disable';

SELECT 'ifNull';
SELECT ifNull(if(number = 0, toNullable('first'), NULL), coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(if(number = 0, toNullable('first'), NULL), coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';

SELECT 'if';
SELECT if(number = 0, 'first', coalesce(CAST('x' AS Enum8('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(number = 0, 'first', coalesce(CAST('x' AS Enum16('x' = 1, 'y' = 2)), intDiv(1, number + 1))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';

SELECT 'nullable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, NULL, NULL) AS Nullable(Enum16('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(multiIf(0 = number, toNullable('first'), NULL), CAST(multiIf(number > -2147483647, NULL, NULL), 'Nullable(Enum16(\'x\' = 1, \'y\' = 2))')) FROM numbers(65535) GROUP BY ALL ORDER BY ALL SETTINGS short_circuit_function_evaluation = 'enable';
