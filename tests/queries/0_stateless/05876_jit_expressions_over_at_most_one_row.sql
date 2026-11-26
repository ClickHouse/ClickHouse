-- Expressions whose input has at most one row (above an aggregation without keys, or above LIMIT 1) are not
-- JIT-compiled: the compilation cannot pay off. Expressions that run on more rows still compile.
-- `CompiledFunctionExecute` counts executions of an already-compiled node, so a warm compiled cache changes nothing.

SELECT sum(number) * 3 + 1, avg(number) + 2 FROM numbers(1000)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_one_row_keyless' FORMAT Null;
SELECT sum(number) * 3 + 1 AS s FROM numbers(1000) ORDER BY s
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_one_row_order_by' FORMAT Null;
SELECT sum(number) * 3 + 1 FROM numbers(1000) HAVING sum(number) > 0
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_one_row_having' FORMAT Null;
SELECT a * 3 + 1 FROM (SELECT number AS a FROM numbers(1000) ORDER BY number LIMIT 1)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_one_row_limit' FORMAT Null;

-- One row per grouping set.
SELECT sum(number) * 3 + 1, avg(number) + 2 FROM numbers(1000) GROUP BY GROUPING SETS ((), ())
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_grouping_sets' FORMAT Null;
SELECT sum(number) * 3 + 1, avg(number) + 2 FROM numbers(1000) GROUP BY number % 10
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_keys' FORMAT Null;
SELECT v * 3 + 1 FROM (SELECT groupArray(number) AS arr FROM numbers(1000)) ARRAY JOIN arr AS v
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_array_join' FORMAT Null;
-- The `arrayJoin` stays in the expression step directly above the aggregation.
SELECT arrayJoin(arr) * 3 + 1 FROM (SELECT groupArray(number) AS arr FROM numbers(1000))
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, query_plan_lower_array_join_function = 0,
    log_comment = '05876_array_join_function' FORMAT Null;
-- The subquery runs once per outer row.
SELECT t.n, l.s, l.a FROM (SELECT number AS n FROM numbers(100)) AS t
    LEFT JOIN LATERAL (SELECT sum(number) * 3 + 1 AS s, avg(number) + 2 AS a FROM numbers(10) WHERE number = t.n) AS l ON true
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, allow_experimental_lateral_join = 1,
    log_comment = '05876_lateral' FORMAT Null;

-- Compiled by a filter step, not an expression step, so it only depends on the embedded compiler being available.
SELECT number FROM numbers(1000) WHERE (number + 1) * 2 > 10
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05876_compiler' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The shapes that must compile are compared against `05876_compiler`: the test stays green on a build without the
-- embedded compiler and turns red when they stop compiling.
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05876\_%'
    GROUP BY log_comment
)
SELECT log_comment, compiled = if(log_comment LIKE '%one\_row%', 0, (SELECT compiled FROM shapes WHERE log_comment = '05876_compiler'))
FROM shapes
WHERE log_comment != '05876_compiler'
ORDER BY log_comment;
