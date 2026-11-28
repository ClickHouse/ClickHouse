SET max_threads = 1, query_plan_execute_functions_after_sorting = 1;

-- An outer ORDER BY must keep each row's number from the subquery.
SELECT number, r
FROM (SELECT number, rowNumberInAllBlocks() AS r FROM numbers(3))
ORDER BY number DESC;

-- The sort for PARTITION BY must also preserve the original row numbers.
SELECT g, r, count() OVER (PARTITION BY g) AS c
FROM (SELECT g, rowNumberInAllBlocks() AS r FROM (SELECT arrayJoin([2, 1, 2]) AS g))
ORDER BY r;
