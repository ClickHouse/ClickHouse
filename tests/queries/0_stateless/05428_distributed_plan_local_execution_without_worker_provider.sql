-- Local execution must not resolve a worker provider (the named cluster does not exist).
-- Zero requested workers leaves the count uncapped: 10 read tasks + one merge task.

SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;
SET ast_fuzzer_runs = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_cascades_optimizer = 0;
SET distributed_plan_fallback_to_local_execution = 0;

DROP TABLE IF EXISTS t_local_no_provider;
CREATE TABLE t_local_no_provider (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_local_no_provider SELECT number FROM numbers(1000000);

SELECT sum(k) FROM t_local_no_provider
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_workers_num = 0,
    distributed_plan_worker_cluster = 'nonexistent_05428', distributed_plan_default_reader_bucket_count = 0,
    distributed_plan_min_rows_per_reader = 100000, log_comment = '05428_local_no_provider';

SYSTEM FLUSH LOGS query_log;

SELECT countIf(query_id != initial_query_id) AS fragments
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND query_id = initial_query_id AND log_comment = '05428_local_no_provider');

DROP TABLE t_local_no_provider;
