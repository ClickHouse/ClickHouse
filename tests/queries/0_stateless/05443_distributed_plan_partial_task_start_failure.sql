-- Tags: no-fasttest, no-parallel
-- no-fasttest: a remote distributed plan needs the stateless worker configuration.
-- no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.

-- The tasks of a stage are started concurrently. A start that fails while others are in flight must report
-- its error. The stage has more tasks than the starts that run at once (16), so some are still queued.

DROP TABLE IF EXISTS t_partial_task_start;

CREATE TABLE t_partial_task_start (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_partial_task_start SELECT number FROM numbers(100000);

SYSTEM ENABLE FAILPOINT distributed_plan_task_start_fails_after_send;

SELECT x, count() FROM t_partial_task_start GROUP BY x FORMAT Null
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 20, distributed_plan_default_reader_bucket_count = 20,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError CANNOT_SCHEDULE_TASK }

SYSTEM DISABLE FAILPOINT distributed_plan_task_start_fails_after_send;

-- Without the injected failure the same query reports every group.
SELECT count() FROM (SELECT x, count() FROM t_partial_task_start GROUP BY x)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 20, distributed_plan_default_reader_bucket_count = 20,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0,
    distributed_plan_fallback_to_local_execution = 0;

DROP TABLE t_partial_task_start;
