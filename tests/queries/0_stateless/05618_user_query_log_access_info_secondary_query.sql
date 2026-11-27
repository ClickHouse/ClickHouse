-- A query shipped to another server, as query text or as a serialized plan, records the tables it read in its own
-- query_log row. Such secondary rows are not part of the initiating user's own log, system.user_query_log.

SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET prefer_localhost_replica = 0;

CREATE TABLE t_remote (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_remote VALUES (1), (2), (3);

SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_remote') SETTINGS serialize_query_plan = 0, log_comment = 'shipped_query' FORMAT Null;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_remote') SETTINGS serialize_query_plan = 1, log_comment = 'shipped_plan' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH (
    SELECT groupArray(query_id) FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('shipped_query', 'shipped_plan')
) AS initial_query_ids
SELECT log_comment, is_initial_query, has(tables, currentDatabase() || '.t_remote') AS table_attributed
FROM system.query_log
WHERE type = 'QueryFinish' AND has(initial_query_ids, initial_query_id) AND is_initial_query = 0
ORDER BY log_comment;

WITH (
    SELECT groupArray(query_id) FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('shipped_query', 'shipped_plan')
) AS initial_query_ids
SELECT log_comment, is_initial_query
FROM system.user_query_log
WHERE type = 'QueryFinish' AND has(initial_query_ids, initial_query_id)
ORDER BY log_comment;
