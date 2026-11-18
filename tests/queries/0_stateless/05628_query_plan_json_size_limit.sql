-- Tags: no-old-analyzer

-- `log_query_plans_max_size` bounds the document written to `system.query_log.query_plan`.
--
-- A query decides how much text its own plan contains: a large `IN` list, a long alias and a
-- folded constant are all rendered into it, and the capture runs with the memory tracker blocked.
-- Without a bound, one query can make a server store, and allocate, an arbitrarily large row.
--
-- A plan that does not fit is not stored truncated, because half a document is not a plan. The
-- column holds the reason instead.

SET log_query_plans = 1;

-- Comfortably within the limit: an ordinary plan is stored whole.
SELECT count() FROM numbers(10)
SETTINGS log_query_plans_max_size = 1048576, log_comment = '05628_small' FORMAT Null;

-- A folded constant lands in the plan's `Output`, which no per-step cap covers, so this is the
-- shape that makes a plan large without the query itself looking large.
SELECT repeat('0123456789', 100000) AS x FROM numbers(1)
SETTINGS log_query_plans_max_size = 4096, log_comment = '05628_over' FORMAT Null;

-- The same query with the limit off is stored whole, which is what shows the row above was
-- rejected for its size rather than for anything else about it.
SELECT repeat('0123456789', 100000) AS x FROM numbers(1)
SETTINGS log_query_plans_max_size = 0, log_comment = '05628_unlimited' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    length(toJSONString(query_plan)) <= 4096 AS within_limit,
    JSONHas(toJSONString(query_plan), 'Error') AS says_why,
    JSONHas(toJSONString(query_plan), 'Nodes') AS has_plan
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('05628_small', '05628_over', '05628_unlimited')
ORDER BY log_comment;
