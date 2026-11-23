-- Skipped short-circuit arguments are filled with valid values of the type, but a null-only tuple field
-- (`Nullable(Nothing)` of a literal `NULL`) keeps `NULL`, because `Nothing` has no default value.

SELECT coalesce(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT coalesce(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(toNullable(tuple('first', NULL)), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(toNullable(tuple('first', NULL)), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT ifNull(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT ifNull(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(if(number = 0, toNullable(tuple('first', NULL)), NULL), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT ifNull(toNullable(tuple('first', NULL)), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(toNullable(tuple('first', NULL)), tuple(CAST(if(number > 0, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT if(number = 0, tuple(CAST('y' AS Enum8('x' = 1, 'y' = 2)), NULL), tuple(CAST(if(number > 1, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT if(number < 3, tuple(CAST('y' AS Enum8('x' = 1, 'y' = 2)), NULL), tuple(CAST(if(number > 1, 'x', 'y') AS Enum8('x' = 1, 'y' = 2)), NULL)) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
