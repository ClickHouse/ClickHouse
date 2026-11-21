-- Tags: no-fasttest
-- no-fasttest: a remote distributed plan needs the stateless worker configuration.

-- Stateless workers read only parts on a remote disk of a shared table, so on remote workers a read of a `MergeTree`
-- table does not distribute: the plan falls back, or throws when the fallback is off. It distributes with local
-- execution, or when the workers are declared to hold the local tables (the workers of the test config are this server).

DROP TABLE IF EXISTS t_unshared;
CREATE TABLE t_unshared (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_unshared SELECT number FROM numbers(1000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_workers_have_local_tables = 0;

SELECT '-- Remote workers, fallback off: the plan throws.';
SELECT sum(x) FROM t_unshared SETTINGS distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- Remote workers, fallback on: the query runs locally.';
SELECT sum(x) FROM t_unshared SETTINGS distributed_plan_execute_locally = 0, log_comment = '05812_remote';

SELECT '-- Local execution: the plan distributes.';
SELECT sum(x) FROM t_unshared SETTINGS distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0, log_comment = '05812_local';

SELECT '-- Remote workers that hold the local tables: the plan distributes.';
SELECT sum(x) FROM t_unshared
SETTINGS distributed_plan_execute_locally = 0, distributed_plan_workers_have_local_tables = 1, distributed_plan_fallback_to_local_execution = 0,
    log_comment = '05812_remote_have_tables';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT '-- The fallback reason of the remote run.';
SELECT replace(replaceRegexpOne(message, '.*falling back to local execution: ', ''), currentDatabase(), '{db}')
FROM system.text_log
WHERE event_date >= yesterday() AND logger_name = 'makeDistributedPlan' AND message LIKE '%falling back to local execution%'
    AND query_id = (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND log_comment = '05812_remote' ORDER BY event_time_microseconds DESC LIMIT 1);

SELECT '-- Which runs had worker tasks (their rows have `<initial query id>::<task>` as query id).';
SELECT i.log_comment, countIf(t.query_id != '') > 0 AS distributed
FROM
(
    SELECT log_comment, query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND log_comment IN ('05812_remote', '05812_local', '05812_remote_have_tables')
) AS i
LEFT JOIN
(
    SELECT initial_query_id, query_id FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query AND query_id LIKE '%::%'
) AS t ON t.initial_query_id = i.query_id
GROUP BY i.log_comment
ORDER BY i.log_comment;

DROP TABLE t_unshared;
