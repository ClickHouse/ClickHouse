-- When a short-circuit argument is skipped for all rows, it is filled with valid values of the type,
-- because it is still converted to the common type together with the used arguments.

SELECT 'coalesce';
SELECT coalesce(if(number < 3, toNullable(tuple('first')), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number < 3, toNullable(tuple('first')), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(if(number < 3, toNullable('first'), NULL), CAST(if(number > 0, 'x', NULL) AS Nullable(Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'ifNull';
SELECT ifNull(if(number < 3, toNullable(tuple('first')), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(if(number < 3, toNullable(tuple('first')), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT ifNull(if(number < 3, toNullable('first'), NULL), CAST(if(number > 0, 'x', NULL) AS Nullable(Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'if';
SELECT if(number < 3, tuple('first'), tuple(CAST(if(number > 0, 'x', 'y') AS Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
