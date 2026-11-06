-- Automatic read task count: one per `distributed_plan_min_rows_per_reader` rows, at most one per worker.
-- The partial aggregation runs in the read tasks, so fragments = read tasks + one merge task.

SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;
SET ast_fuzzer_runs = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_cascades_optimizer = 0;
SET distributed_plan_fallback_to_local_execution = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 8;

DROP TABLE IF EXISTS t_auto_readers;
CREATE TABLE t_auto_readers (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192;
INSERT INTO t_auto_readers SELECT number FROM numbers(1000000);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4;

SELECT sum(k) FROM t_auto_readers SETTINGS distributed_plan_min_rows_per_reader = 100000, log_comment = '05427_capped_by_workers';
SELECT sum(k) FROM t_auto_readers SETTINGS distributed_plan_min_rows_per_reader = 400000, log_comment = '05427_by_rows';
SELECT sum(k) FROM t_auto_readers SETTINGS distributed_plan_min_rows_per_reader = 2000000, log_comment = '05427_one_reader';
SELECT sum(k) FROM t_auto_readers SETTINGS distributed_plan_default_reader_bucket_count = 3, log_comment = '05427_explicit';

-- A worker runs no more threads than it has cores, whatever `max_threads` the initiator sends.
SET distributed_plan_execute_locally = 0, distributed_plan_workers_num = 0;
SELECT sum(k) FROM t_auto_readers SETTINGS max_threads = 1024, log_comment = '05427_max_threads';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

SELECT q.log_comment, countIf(f.query_id != f.initial_query_id) AS fragments
FROM
(
    SELECT log_comment, argMax(query_id, event_time_microseconds) AS initiator_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND query_id = initial_query_id AND log_comment IN ('05427_capped_by_workers', '05427_by_rows', '05427_one_reader', '05427_explicit')
    GROUP BY log_comment
) AS q
INNER JOIN system.query_log AS f ON f.initial_query_id = q.initiator_id
WHERE f.event_date >= yesterday() AND f.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

SELECT count() > 0, max(toUInt64OrZero(Settings['max_threads'])) BETWEEN 1 AND 1023
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND query_id != initial_query_id
    AND initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND query_id = initial_query_id AND log_comment = '05427_max_threads');

DROP TABLE t_auto_readers;
