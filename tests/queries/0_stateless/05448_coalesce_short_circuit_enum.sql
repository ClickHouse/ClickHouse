-- The rows skipped by short-circuit evaluation must be filled with valid values of the type,
-- because they are converted to the common type together with the used rows.

SELECT 'coalesce';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', NULL) AS Nullable(Enum16('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable('first'), NULL), if(number = 1, toNullable('second'), NULL), CAST(if(number > 1, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'ifNull';
SELECT ifNull(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT ifNull(if(number = 0, toNullable('first'), NULL), CAST(if(number > 0, 'x', NULL) AS Nullable(Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'if';
SELECT if(number = 0, 'first', CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(number = 0, tuple('first'), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT multiIf(number = 0, 'first', CAST(if(number > 0, 'x', NULL) AS Nullable(Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
