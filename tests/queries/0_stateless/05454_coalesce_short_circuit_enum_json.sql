-- The rows of a short-circuit argument that are skipped are filled with valid values of the type, also in a typed path
-- of `JSON`, because they are still converted to the common type together with the used rows.

SELECT 'coalesce';
SELECT coalesce(if(number = 0, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(if(number < 3, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'ifNull';
SELECT ifNull(if(number = 0, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT ifNull(if(number = 0, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT ifNull(if(number < 3, toNullable(CAST('{"a":"first"}' AS JSON(a String))), NULL), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Nullable(Enum8('x' = 1, 'y' = 2))))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'if';
SELECT if(number = 0, CAST('{"a":"first"}' AS JSON(a String)), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'force_enable';

-- `multiIf` skips the arguments after the branch that takes all rows, but still converts them to the common type.
SELECT 'multiIf';
SELECT multiIf(number < 3, CAST('{"a":"first"}' AS JSON(a String)), number < 5, CAST('{"a":"second"}' AS JSON(a String)), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) ORDER BY number SETTINGS short_circuit_function_evaluation = 'enable';

-- The final branch is skipped entirely after the second condition.
SELECT multiIf(number < 2, CAST('{"a":"first"}' AS JSON(a String)), number < 5, CAST('{"a":"second"}' AS JSON(a String)), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) ORDER BY number SETTINGS short_circuit_function_evaluation = 'enable';

-- Control case: the final branch is partially evaluated through the maskedExecute path.
SELECT multiIf(number < 1, CAST('{"a":"first"}' AS JSON(a String)), number < 2, CAST('{"a":"second"}' AS JSON(a String)), CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))) FROM numbers(3) ORDER BY number SETTINGS short_circuit_function_evaluation = 'enable';

-- `CASE x WHEN` uses caseWithExpression, which falls back to `multiIf`.
SELECT CASE number % 1
    WHEN 0 THEN CAST('{"a":"first"}' AS JSON(a String))
    ELSE CAST(if(number > 0, '{"a":"x"}', '{"a":"y"}') AS JSON(a Enum8('x' = 1, 'y' = 2)))
END FROM numbers(3) ORDER BY number SETTINGS short_circuit_function_evaluation = 'enable';
