-- The heavy first argument of `and` is not moved behind the cheap one if its lazily executed part has other consumers,
-- because each of them would execute it again. Each query runs without and with reordering; the number of function executions
-- must be the same.

-- The heavy argument is shared by several `and`.
SELECT isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 2,
    isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 3
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_shared_0'
FORMAT Null;
SELECT isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 2,
    isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 3
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_shared_1'
FORMAT Null;

-- A part of the heavy argument is shared with a branch of `if`.
SELECT isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, if(number % 10 != 2, length(repeat(toString(number), 10)), 0)
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_descendant_0'
FORMAT Null;
SELECT isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, if(number % 10 != 2, length(repeat(toString(number), 10)), 0)
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_descendant_1'
FORMAT Null;

-- The heavy argument is shared by two `and` that are executed lazily, in branches of `if`. It is executed by each of them
-- in either order, so they still execute the cheap argument first.
SELECT if(number % 2 = 0, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, 0),
    if(number % 3 = 0, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 2, 0)
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_nested_0'
FORMAT Null;
SELECT if(number % 2 = 0, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 1, 0),
    if(number % 3 = 0, isValidUTF8(repeat(toString(number), 10)) AND number % 10 != 2, 0)
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_nested_1'
FORMAT Null;

-- A shared part that is an output is executed on all rows anyway, so the rest of the heavy argument is still moved.
SELECT repeat(toString(number), 10) AS r, isValidUTF8(r) AND number % 10 != 1
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_output_0'
FORMAT Null;
SELECT repeat(toString(number), 10) AS r, isValidUTF8(r) AND number % 10 != 1
FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    max_threads = 1, max_block_size = 1000, log_comment = '05401_output_1'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, 1, length(log_comment) - 2) AS name,
    countIf(endsWith(log_comment, '_0')) AS runs_without_reordering,
    countIf(endsWith(log_comment, '_1')) AS runs_with_reordering,
    anyIf(ProfileEvents['FunctionExecute'], endsWith(log_comment, '_0'))
        = anyIf(ProfileEvents['FunctionExecute'], endsWith(log_comment, '_1')) AS same_function_executions,
    anyIf(ProfileEvents['ShortCircuitArgumentsReordered'], endsWith(log_comment, '_1')) > 0 AS reordered
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05401_%'
GROUP BY name
ORDER BY name;
