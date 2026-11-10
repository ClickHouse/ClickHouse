-- Tags: no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace the allocator, so the copies of the plan are not counted against `max_memory_usage`

-- A query that hits `max_memory_usage` while the local tasks of a distributed plan stage are being started
-- must fail with the error instead of aborting the server.

DROP TABLE IF EXISTS t_local_stage_start;
CREATE TABLE t_local_stage_start (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_local_stage_start SELECT number, toString(number) FROM numbers(100);

-- Each task gets its own copy of the stage's plan, which carries the 1 MB constant.
SELECT k FROM t_local_stage_start WHERE s != repeat('x', 1000000) ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 32,
    max_untracked_memory = 0, max_memory_usage = 20000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- With one task the same query fits in the same limit.
SELECT k FROM t_local_stage_start WHERE s != repeat('x', 1000000) ORDER BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 1,
    max_untracked_memory = 0, max_memory_usage = 20000000;

DROP TABLE t_local_stage_start;
