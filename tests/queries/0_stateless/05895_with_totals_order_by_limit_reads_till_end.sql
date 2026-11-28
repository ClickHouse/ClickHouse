-- The totals row of WITH TOTALS covers every group, whatever the ORDER BY and the LIMIT.
SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY g WITH TOTALS ORDER BY g LIMIT 0;
SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY g WITH TOTALS ORDER BY g LIMIT 1, 0;
SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY g WITH TOTALS ORDER BY g LIMIT 0 WITH TIES;
SELECT number % 3 AS g, count() AS c FROM numbers(10) GROUP BY g WITH TOTALS HAVING c > 3 ORDER BY g LIMIT 0;
SELECT count() FROM numbers(10) WITH TOTALS ORDER BY 1 LIMIT 0;
SELECT number % 3 AS g, count() FROM remote('127.0.0.{1,2}', numbers(10)) GROUP BY g WITH TOTALS ORDER BY g LIMIT 0;
-- A constant ORDER BY does not sort: neither the LIMIT nor the merge of the sorted streams may stop before the last group.
SELECT count() FROM numbers(100000) GROUP BY number WITH TOTALS ORDER BY NULL LIMIT 1 SETTINGS max_block_size = 128, max_threads = 1;
SELECT count() FROM numbers(100000) GROUP BY number WITH TOTALS ORDER BY NULL LIMIT 1 SETTINGS max_block_size = 128, max_threads = 4;
SELECT c FROM (SELECT number AS g, count() AS c FROM numbers(100000) GROUP BY g WITH TOTALS) ORDER BY NULL LIMIT 1 SETTINGS max_block_size = 128, max_threads = 4;
SELECT DISTINCT count() FROM numbers(100000) GROUP BY number WITH TOTALS ORDER BY NULL LIMIT 1 SETTINGS max_block_size = 128, query_plan_remove_redundant_distinct = 0;
SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY g WITH TOTALS ORDER BY g LIMIT 0 AFTER g >= 0;
SELECT number AS g, count() FROM numbers(100000) GROUP BY g WITH TOTALS ORDER BY NULL LIMIT UNTIL g >= 0 SETTINGS max_block_size = 128;
-- WITH FILL runs above the sort: the totals must not make the LIMIT generate the remaining fill rows.
SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY g WITH TOTALS ORDER BY g WITH FILL LIMIT 0;
SELECT DISTINCT number * 2 AS g, count() AS c FROM numbers(2) GROUP BY g WITH TOTALS ORDER BY g WITH FILL STEP 1 INTERPOLATE (c AS if(g > 0, throwIf(1), c)) LIMIT 1 SETTINGS max_threads = 1, max_block_size = 1, short_circuit_function_evaluation = 'enable';
-- An error in a group read only for the totals still fails the query.
SELECT number AS g, count() FROM numbers(100000) GROUP BY g WITH TOTALS HAVING throwIf(rowNumberInAllBlocks() >= 50000) = 0 ORDER BY NULL LIMIT 1 SETTINGS max_block_size = 128, max_threads = 1 FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- Discarded totals are not computed.
SELECT count(), sum(c) FROM ((SELECT number % 3 AS g, count() AS c FROM numbers(10) GROUP BY g WITH TOTALS HAVING throwIf(g = 2) = 0 ORDER BY g LIMIT 0) UNION ALL (SELECT 0, sleepEachRow(0.01) FROM numbers(30))) SETTINGS max_block_size = 1;
