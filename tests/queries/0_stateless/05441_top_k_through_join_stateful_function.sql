-- a stateful function between the sort and the join must see all rows, so the sort does not move below the join
SET query_plan_execute_functions_after_sorting = 0, max_threads = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT t1.number, rowNumberInAllBlocks() AS rn FROM numbers(10) AS t1 LEFT JOIN numbers(5) AS t2 ON t1.number = t2.number ORDER BY t1.number DESC LIMIT 2 SETTINGS query_plan_top_k_through_join = 0;
SELECT t1.number, rowNumberInAllBlocks() AS rn FROM numbers(10) AS t1 LEFT JOIN numbers(5) AS t2 ON t1.number = t2.number ORDER BY t1.number DESC LIMIT 2 SETTINGS query_plan_top_k_through_join = 1;
