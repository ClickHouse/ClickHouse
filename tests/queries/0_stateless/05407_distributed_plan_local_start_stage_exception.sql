-- Tags: no-fasttest, no-parallel
-- no-fasttest, no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.

-- An exception while the local tasks of a distributed plan stage are being started, after some of them
-- started, must fail the query instead of aborting the server: the started task threads must be joined.

DROP TABLE IF EXISTS t_local_stage_start;
CREATE TABLE t_local_stage_start (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_local_stage_start SELECT number, toString(number) FROM numbers(100);

SYSTEM ENABLE FAILPOINT distributed_plan_local_start_stage_throws_after_task_start;

SELECT k FROM t_local_stage_start ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 32; -- { serverError CANNOT_SCHEDULE_TASK }

SYSTEM DISABLE FAILPOINT distributed_plan_local_start_stage_throws_after_task_start;

-- Without the exception the same query succeeds.
SELECT k FROM t_local_stage_start ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 32;

DROP TABLE t_local_stage_start;
