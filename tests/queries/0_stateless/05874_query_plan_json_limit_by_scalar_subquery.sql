-- Tags: no-old-analyzer

-- A scalar subquery that gives the bound of `LIMIT ... BY`, or the count of `LIMIT ... AFTER`, is
-- linked to the step that applies it. Those steps keep the bound as a plain number, not as an
-- expression, so the planner puts the link on them directly, as it does for an ordinary `LIMIT`.

SET enable_parallel_replicas = 0;

SET log_query_plans = 1;

SELECT number % 3 AS k FROM numbers(10) LIMIT (SELECT count() FROM numbers(2)) BY k
SETTINGS log_comment = '05874_limit_by' FORMAT Null;

SELECT number % 3 AS k FROM numbers(10) LIMIT 1 OFFSET (SELECT count() FROM numbers(1)) BY k
SETTINGS log_comment = '05874_limit_by_offset' FORMAT Null;

SELECT number FROM numbers(10) ORDER BY number LIMIT (SELECT count() FROM numbers(3)) AFTER number >= 3
SETTINGS log_comment = '05874_limit_after' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- For each query: the number of plans, and the types of the nodes of the query's plan that link
-- to a sub-plan.
SELECT
    replaceOne(log_comment, '05874_', '') AS shape,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') AS sub_plan_kind,
    arrayFilter(
        t -> t != '',
        arrayMap(
            n -> if(position(n, '"SubPlans"') > 0, JSONExtractString(n, 'NodeType'), ''),
            JSONExtractArrayRaw(toJSONString(query_plan), 'Plans', 1, 'Nodes'))) AS consumers
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05874_limit_by', '05874_limit_by_offset', '05874_limit_after')
ORDER BY shape;
