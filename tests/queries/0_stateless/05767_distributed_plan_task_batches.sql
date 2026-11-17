-- With `distributed_plan_task_dispatch_batching = 1` the coordinator sends each worker's tasks in `start_tasks` batches, each
-- stage's plan once per batch, and with 0 one task per batch; the results must equal local execution, with streaming and
-- persisted exchanges, whether the batches go to one worker after another (1 thread) or to several workers at once (8), and
-- whether they are handed over after every stage (`distributed_plan_task_dispatch_stages_per_flush = 1`).

SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_cascades_optimizer = 0;
SET distributed_plan_fallback_to_local_execution = 0;
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;

DROP TABLE IF EXISTS t_task_batches;
CREATE TABLE t_task_batches (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_task_batches SELECT number % 1000, number FROM numbers(100000);

SELECT 'local';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k);
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v;
SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000));

SELECT 'aggregation Streaming';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 0,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Streaming-0-1-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Streaming-1-1-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Streaming-1-8-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_task_dispatch_stages_per_flush = 1,
    distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Streaming-1-8-1';

SELECT 'join Streaming';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 0,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Streaming-0-1-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Streaming-1-1-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Streaming-1-8-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_task_dispatch_stages_per_flush = 1,
    distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Streaming-1-8-1';

SELECT 'set Streaming';
SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 0,
    distributed_plan_task_dispatch_threads = 1, log_comment = 'task-batches-set-Streaming-0-1-0';
SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 1, log_comment = 'task-batches-set-Streaming-1-1-0';
SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, log_comment = 'task-batches-set-Streaming-1-8-0';
SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000))
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_task_dispatch_stages_per_flush = 1,
    log_comment = 'task-batches-set-Streaming-1-8-1';

SELECT 'aggregation Persisted';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 0,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Persisted-0-1-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Persisted-1-1-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Persisted-1-8-0';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_task_dispatch_stages_per_flush = 1,
    distributed_plan_force_shuffle_aggregation = 1, log_comment = 'task-batches-aggregation-Persisted-1-8-1';

SELECT 'join Persisted';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 0,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Persisted-0-1-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 1, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Persisted-1-1-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Persisted-1-8-0';
SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v
SETTINGS make_distributed_plan = 1, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_task_dispatch_batching = 1,
    distributed_plan_task_dispatch_threads = 8, distributed_plan_task_dispatch_stages_per_flush = 1,
    distributed_plan_max_rows_to_broadcast = 0, log_comment = 'task-batches-join-Persisted-1-8-1';

-- The same remote tasks in every arm; with batching fewer batches than tasks, since a worker's tasks share one, else one each.
-- A flush after every stage gives more batches than one flush with streaming exchanges; persisted exchanges already flush before
-- each wait, so only producers started together (the two sides of a join) split.
SYSTEM FLUSH LOGS query_log;
SELECT shape, kind, batching, threads, per_flush, remote_tasks > 0, remote_tasks = max(remote_tasks) OVER (PARTITION BY shape, kind),
    batches > 0, if(batching = '1', batches < remote_tasks, batches = remote_tasks),
    if(per_flush = '1', if(kind = 'Streaming', batches > one_flush_batches, batches >= one_flush_batches), 1)
FROM
(
    SELECT splitByChar('-', log_comment) AS parts, parts[3] AS shape, parts[4] AS kind, parts[5] AS batching, parts[6] AS threads,
        parts[7] AS per_flush, ProfileEvents['DistributedPlanRemoteTasks'] AS remote_tasks,
        ProfileEvents['DistributedPlanTaskBatches'] AS batches,
        maxIf(batches, batching = '1' AND threads = '8' AND per_flush = '0') OVER (PARTITION BY shape, kind) AS one_flush_batches
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND query_id = initial_query_id AND log_comment LIKE 'task-batches-%'
)
ORDER BY shape, kind, batching, threads, per_flush;

DROP TABLE t_task_batches;
