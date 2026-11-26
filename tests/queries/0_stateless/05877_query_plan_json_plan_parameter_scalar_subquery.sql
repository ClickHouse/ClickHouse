-- Tags: no-old-analyzer

-- A scalar subquery whose value becomes a parameter that a step keeps as a plain value, not in an
-- `ActionsDAG`, is linked to that step: a window frame bound, a parameter of an aggregate function
-- or of an aggregate used as a window function, and a `WITH FILL` bound.

SET enable_parallel_replicas = 0;

SET log_query_plans = 1;

SELECT sum(number) OVER (ORDER BY number ROWS BETWEEN (SELECT count() FROM numbers(2)) PRECEDING AND CURRENT ROW)
FROM numbers(10)
SETTINGS log_comment = '05877_window_frame' FORMAT Null;

SELECT quantileExact((SELECT 0.5))(number) FROM numbers(10)
SETTINGS log_comment = '05877_aggregate_parameter' FORMAT Null;

SELECT quantileExact((SELECT 0.5))(number) OVER () FROM numbers(10)
SETTINGS log_comment = '05877_window_function_parameter' FORMAT Null;

SELECT number FROM numbers(3) ORDER BY number WITH FILL TO assumeNotNull((SELECT count() FROM numbers(6)))
SETTINGS log_comment = '05877_with_fill' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- For each query: the number of plans, and the types of the nodes of the query's plan that link
-- to a sub-plan.
SELECT
    replaceOne(log_comment, '05877_', '') AS shape,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') AS sub_plan_kind,
    arrayFilter(
        t -> t != '',
        arrayMap(
            n -> if(position(n, '"SubPlans"') > 0, JSONExtractString(n, 'NodeType'), ''),
            JSONExtractArrayRaw(toJSONString(query_plan), 'Plans', 1, 'Nodes'))) AS consumers
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05877_window_frame', '05877_aggregate_parameter', '05877_window_function_parameter', '05877_with_fill')
ORDER BY shape;
