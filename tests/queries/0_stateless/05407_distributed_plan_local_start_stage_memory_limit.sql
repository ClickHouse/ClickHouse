-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the failpoint is global.
-- Tag no-fasttest: a test that enables a failpoint runs alone, which is left to the full stateless jobs.

-- An exception while the local tasks of a distributed plan stage are being started must fail the query instead of
-- aborting the server. The tasks once copied the stage's plan and hit `max_memory_usage` here; they share one copy now,
-- so the exception is injected after the first task has started.

DROP TABLE IF EXISTS t_local_stage_start;
CREATE TABLE t_local_stage_start (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_local_stage_start SELECT number, toString(number) FROM numbers(100);

SYSTEM ENABLE FAILPOINT distributed_plan_local_start_stage_fails_after_first_task;
SELECT k FROM t_local_stage_start WHERE s != repeat('x', 1000000) ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 32; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT distributed_plan_local_start_stage_fails_after_first_task;

-- The 32 tasks share one copy of the 1 MB plan, so the query fits in the limit one task needs.
SELECT k FROM t_local_stage_start WHERE s != repeat('x', 1000000) ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 32,
    max_untracked_memory = 0, max_memory_usage = 20000000;

DROP TABLE t_local_stage_start;
