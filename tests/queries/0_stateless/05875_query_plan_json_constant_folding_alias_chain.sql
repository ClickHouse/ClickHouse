-- Tags: no-old-analyzer

-- Constant folding carries the ids of the scalar subqueries folded into a constant onto the result.
-- A constant already holds the ids of everything folded into it, so they are taken from it, and
-- each node is visited once: aliases make the query tree share nodes, and in a chain where each
-- alias doubles the one before, a walk that expanded the shared history again at every fold would
-- visit an exponential number of nodes.
--
-- Without plan logging nothing reads the ids, so they are not collected at all. With it, a chain
-- that starts from a scalar subquery still links the step reading the folded value to the subquery.

-- The expanded tree of the chain is larger than the default limit.
SET max_expanded_ast_elements = 1000000000;
SET enable_parallel_replicas = 0;

WITH 1 AS x0,
    x0 + x0 AS x1,
    x1 + x1 AS x2,
    x2 + x2 AS x3,
    x3 + x3 AS x4,
    x4 + x4 AS x5,
    x5 + x5 AS x6,
    x6 + x6 AS x7,
    x7 + x7 AS x8,
    x8 + x8 AS x9,
    x9 + x9 AS x10,
    x10 + x10 AS x11,
    x11 + x11 AS x12,
    x12 + x12 AS x13,
    x13 + x13 AS x14,
    x14 + x14 AS x15,
    x15 + x15 AS x16,
    x16 + x16 AS x17,
    x17 + x17 AS x18,
    x18 + x18 AS x19,
    x19 + x19 AS x20,
    x20 + x20 AS x21,
    x21 + x21 AS x22,
    x22 + x22 AS x23,
    x23 + x23 AS x24,
    x24 + x24 AS x25,
    x25 + x25 AS x26
SELECT x26
SETTINGS log_query_plans = 0;

WITH (SELECT 1) AS x0,
    x0 + x0 AS x1,
    x1 + x1 AS x2,
    x2 + x2 AS x3,
    x3 + x3 AS x4,
    x4 + x4 AS x5,
    x5 + x5 AS x6,
    x6 + x6 AS x7,
    x7 + x7 AS x8,
    x8 + x8 AS x9,
    x9 + x9 AS x10,
    x10 + x10 AS x11,
    x11 + x11 AS x12,
    x12 + x12 AS x13,
    x13 + x13 AS x14,
    x14 + x14 AS x15,
    x15 + x15 AS x16,
    x16 + x16 AS x17,
    x17 + x17 AS x18,
    x18 + x18 AS x19,
    x19 + x19 AS x20
SELECT count() FROM numbers(10) WHERE number < x20
SETTINGS log_query_plans = 1, log_comment = '05875_scalar_chain';

SYSTEM FLUSH LOGS query_log;

-- The plan of the scalar subquery, and one link to it, from the step that reads the folded value.
SELECT
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') AS sub_plan_kind,
    countSubstrings(toJSONString(query_plan), '"SubPlans"') AS links
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05875_scalar_chain';
