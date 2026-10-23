-- additional_result_filter must not filter the extremes rows (issue #125580).
SET extremes = 1;

SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'a + b >= 2';
SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'a + b = 2';
SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'intDiv(1, a + b) = 0 AND b >= 1', query_plan_merge_filters = 1;
SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'a IN (SELECT 1)';
SELECT number AS a FROM numbers(3) UNION ALL SELECT number + 10 AS a FROM numbers(3) SETTINGS additional_result_filter = 'a = 11';
SELECT number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'a >= 1', output_format_write_statistics = 0 FORMAT JSONColumnsWithMetadata;
SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'intDiv(1, a + b) = 0';
SELECT number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'arrayJoin([a > 0])';
SELECT number AS a, 2 - number AS b FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'arrayJoin([intDiv(1, a + b) = 0])';
SELECT number - 1 AS d FROM numbers(3) ORDER BY d SETTINGS additional_result_filter = 'd >= 0 AND intDiv(1, d + 1) >= 0', short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1;
SELECT number - 1 AS d FROM numbers(3) ORDER BY d SETTINGS additional_result_filter = 'd >= 0 AND arrayJoin([intDiv(1, d + 1)]) >= 0', query_plan_merge_filters = 1;
SELECT number AS b, [1, 2] AS arr FROM numbers(2) ORDER BY b SETTINGS additional_result_filter = 'arrayExists(x -> x > b, arr) AND arrayAll(x -> x > b, arr)', query_plan_merge_filters = 1;
SELECT number AS a, 2 - number AS b, [1, 2] AS arr FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'arrayExists(x -> x > intDiv(1, a + b), arr) AND arrayAll(x -> x > intDiv(1, a + b), arr)', query_plan_merge_filters = 1;
SELECT 0 AS c, [1, 2] AS arr, number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'arrayExists(x -> x > c, arr) AND arrayAll(x -> x > c, arr)', query_plan_merge_filters = 1;
SELECT number AS a FROM numbers(1) SETTINGS additional_result_filter = 'arrayJoin([0, 1]) = rowNumberInBlock()', query_plan_lower_array_join_function = 1, legacy_array_join_function_nondeterministic_evaluation = 0, max_threads = 1, max_block_size = 8192;
SELECT number AS a FROM numbers(2) SETTINGS additional_result_filter = 'arrayJoin(if(a = 0, emptyArrayUInt8(), [1])) = intDiv(1, a)', query_plan_lift_up_array_join = 0, max_threads = 1;
SELECT number AS a FROM numbers(2) SETTINGS additional_result_filter = 'arrayJoin(if(a = 0, emptyArrayUInt8(), [1])) = intDiv(1, a)', query_plan_lift_up_array_join = 0, max_threads = 1, extremes = 0;
SELECT number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'a >= 1 AND arrayJoin([a, a + 1]) > 1', query_plan_merge_filters = 1;
SELECT number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'arrayJoin(emptyArrayToSingle(if(a = 0, emptyArrayUInt64(), [a]))) >= 0';
SELECT number AS a FROM numbers(3) SETTINGS additional_result_filter = 'arrayJoin([0, 1, 2]) = rowNumberInBlock()', max_threads = 1, max_block_size = 8192;
