-- arrayJoin is planned as an ARRAY JOIN step, also without plan optimizations

SET query_plan_lower_array_join_function = 1;
SET legacy_array_join_function_nondeterministic_evaluation = 0;

SELECT '-- projection';
SELECT count() FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS x, number FROM numbers(2) SETTINGS query_plan_enable_optimizations = 0)
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

SELECT '-- two independent calls give two steps';
SELECT count() FROM (EXPLAIN SELECT arrayJoin([1, 2]) AS x, arrayJoin([3, 4]) AS y FROM numbers(1) SETTINGS query_plan_enable_optimizations = 0)
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

SELECT '-- WHERE';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(3) WHERE arrayJoin([number, 1]) = 1 SETTINGS query_plan_enable_optimizations = 0)
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

SELECT '-- additional_result_filter';
SELECT count() FROM (EXPLAIN SELECT number FROM numbers(3) SETTINGS additional_result_filter = 'arrayJoin([number, 1]) = 1', query_plan_enable_optimizations = 0)
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

SELECT '-- query_plan_lower_array_join_function = 0 keeps the function';
SELECT count() FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS x FROM numbers(2) SETTINGS query_plan_enable_optimizations = 0, query_plan_lower_array_join_function = 0)
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

SELECT '-- same rows with and without the step';
SELECT number, arrayJoin([number, 1]) AS x, arrayJoin(range(number)) AS y FROM numbers(3) ORDER BY ALL SETTINGS query_plan_enable_optimizations = 0;
SELECT number, arrayJoin([number, 1]) AS x, arrayJoin(range(number)) AS y FROM numbers(3) ORDER BY ALL SETTINGS query_plan_enable_optimizations = 0, query_plan_lower_array_join_function = 0;
SELECT number FROM numbers(3) ORDER BY number SETTINGS additional_result_filter = 'arrayJoin([number, 1]) = 1', query_plan_enable_optimizations = 0;
SELECT number FROM numbers(3) ORDER BY number SETTINGS additional_result_filter = 'arrayJoin([number, 1]) = 1', query_plan_enable_optimizations = 0, query_plan_lower_array_join_function = 0;
SELECT arrayJoin(if(number IN (SELECT 1), [10, 20], [30])) AS x FROM numbers(2) ORDER BY x SETTINGS query_plan_enable_optimizations = 0;
