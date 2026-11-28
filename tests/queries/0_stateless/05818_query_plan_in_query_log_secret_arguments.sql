-- Tags: no-old-analyzer, no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- A query with secret arguments stores no plan in `system.query_log.query_plan`. The `query`
-- column hides them by their position in the AST, but in the plan they are constants inside
-- expressions, possibly folded into derived values, so they cannot be reliably hidden there.

SET log_query_plans = 1;

SELECT hex(encrypt('aes-128-ecb', toString(number), 'mysecretkey12345')) FROM numbers(2)
SETTINGS log_comment = '05818_secret' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

-- The column takes its default, an empty object.
SELECT count(), anyLast(toJSONString(query_plan)), countIf(position(query, 'mysecretkey12345') > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05818_secret';
