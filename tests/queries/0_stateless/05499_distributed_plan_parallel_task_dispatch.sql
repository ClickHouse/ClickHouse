-- With `distributed_plan_task_dispatch_threads` > 0 the coordinator sends the `start` requests of a stage's tasks from a pool
-- of senders; the results must equal the serial dispatch (0) and local execution.

SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_cascades_optimizer = 0;
SET distributed_plan_fallback_to_local_execution = 0;

DROP TABLE IF EXISTS t_parallel_dispatch;
CREATE TABLE t_parallel_dispatch (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_parallel_dispatch SELECT number % 1000, number FROM numbers(100000);

SELECT 'local';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_parallel_dispatch GROUP BY k);
SELECT count(), sum(a.k) FROM t_parallel_dispatch AS a INNER JOIN t_parallel_dispatch AS b ON a.v = b.v;
SELECT count() FROM t_parallel_dispatch WHERE v IN (SELECT number * 2 FROM numbers(50000));

-- Shuffle aggregation: 4 read tasks feed 4 aggregation tasks over streaming exchanges.
SELECT 'aggregation';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_parallel_dispatch GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 0, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 4, distributed_plan_force_shuffle_aggregation = 1,
    log_comment = '05499-aggregation-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_parallel_dispatch GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 4, distributed_plan_force_shuffle_aggregation = 1,
    log_comment = '05499-aggregation-1';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_parallel_dispatch GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 8, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 4, distributed_plan_force_shuffle_aggregation = 1,
    log_comment = '05499-aggregation-8';

-- Shuffle join: two read stages feed one join stage.
SELECT 'join';
SELECT count(), sum(a.k) FROM t_parallel_dispatch AS a INNER JOIN t_parallel_dispatch AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 0, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 4, distributed_plan_max_rows_to_broadcast = 0,
    log_comment = '05499-join-0';
SELECT count(), sum(a.k) FROM t_parallel_dispatch AS a INNER JOIN t_parallel_dispatch AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 8, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 4, distributed_plan_max_rows_to_broadcast = 0,
    log_comment = '05499-join-8';

-- A set shipped in every read task's description.
SELECT 'set';
SELECT count() FROM t_parallel_dispatch WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 0, distributed_plan_default_reader_bucket_count = 4,
    log_comment = '05499-set-0';
SELECT count() FROM t_parallel_dispatch WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_task_dispatch_threads = 8, distributed_plan_default_reader_bucket_count = 4,
    log_comment = '05499-set-8';

-- Every query ran remote tasks: sent one by one (0 threads), or queued for the senders (1, 8 threads).
SYSTEM FLUSH LOGS query_log, text_log;
SELECT q.log_comment, q.remote_tasks > 0, countIf(t.message LIKE 'Queueing task %') > 0, countIf(t.message LIKE 'Sending task %') > 0
FROM
(
    SELECT query_id, log_comment, ProfileEvents['DistributedPlanRemoteTasks'] AS remote_tasks
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND query_id = initial_query_id AND log_comment LIKE '05499-%'
) AS q
LEFT JOIN
(
    SELECT query_id, message FROM system.text_log
    WHERE event_date >= yesterday() AND (message LIKE 'Queueing task %' OR message LIKE 'Sending task %')
) AS t ON t.query_id = q.query_id
GROUP BY q.log_comment, q.remote_tasks
ORDER BY q.log_comment;

DROP TABLE t_parallel_dispatch;
