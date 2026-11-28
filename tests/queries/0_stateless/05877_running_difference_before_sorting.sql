SET max_threads = 1, max_block_size = 3, allow_deprecated_error_prone_window_functions = 1,
    query_plan_execute_functions_after_sorting = 1;

-- Compute `runningDifference` on the subquery's input before reversing the rows.
SELECT number, d
FROM (SELECT number, runningDifference(toInt64(number)) AS d FROM numbers(3))
ORDER BY number DESC;
