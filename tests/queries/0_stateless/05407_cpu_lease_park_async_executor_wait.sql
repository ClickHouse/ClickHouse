-- Tags: no-parallel
-- no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time.

DROP TABLE IF EXISTS t_rows;
DROP TABLE IF EXISTS t_heavy;
DROP WORKLOAD IF EXISTS 05407_wl;
DROP RESOURCE IF EXISTS 05407_cpu;
CREATE RESOURCE 05407_cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD 05407_wl SETTINGS max_concurrent_threads = 1;

CREATE TABLE t_rows (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_heavy (x UInt64, y Array(UInt64) MATERIALIZED arrayMap(i -> cityHash64(x, i), range(300))) ENGINE = MergeTree ORDER BY x;

-- Each query waits for another pipeline of the same workload, which can only run on the slot the waiting thread gives back.

-- Several blocks: the asynchronous INSERT ... SELECT pushes them into its own insert pipeline.
INSERT INTO t_rows SELECT number FROM numbers(1000000)
SETTINGS workload = '05407_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, max_block_size = 65409,
    max_execution_time = 60, async_insert = 1, async_insert_select_as_async_insert = 1, wait_for_async_insert = 1,
    async_insert_max_data_size = 10485760, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 200,
    log_comment = '05407_h1';

-- One block: it goes to the asynchronous insert queue, and the query waits for the flush.
INSERT INTO t_heavy SELECT number FROM numbers(20000)
SETTINGS workload = '05407_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, max_block_size = 65409,
    max_execution_time = 60, async_insert = 1, async_insert_select_as_async_insert = 1, wait_for_async_insert = 1,
    async_insert_max_data_size = 10485760, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 200,
    log_comment = '05407_h2';

-- Each member of the recursive CTE runs in its own pipeline.
WITH RECURSIVE r AS
(
    SELECT number AS n, 0 AS d FROM numbers(1000000)
    UNION ALL
    SELECT n + 1, d + 1 FROM r WHERE d < 1 AND n < 10
)
SELECT count(), sum(cityHash64(arrayMap(i -> cityHash64(n, i), range(20)))) FROM r
SETTINGS workload = '05407_wl', max_threads = 1, use_concurrency_control = 1, max_block_size = 65409, max_execution_time = 60,
    log_comment = '05407_h3';

SELECT count() FROM t_rows;
SELECT count() FROM t_heavy;

SYSTEM FLUSH LOGS query_log, asynchronous_insert_log;

-- The first INSERT used its insert pipeline, the second one the queue.
SELECT countIf(table = 't_rows'), countIf(table = 't_heavy')
FROM system.asynchronous_insert_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND status = 'Ok';

-- The waiting threads parked their CPU leases.
SELECT log_comment, ProfileEvents['ConcurrencyControlParks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND query_kind IN ('Insert', 'Select') AND log_comment IN ('05407_h1', '05407_h2', '05407_h3')
ORDER BY log_comment;

DROP TABLE t_rows;
DROP TABLE t_heavy;
DROP WORKLOAD 05407_wl;
DROP RESOURCE 05407_cpu;
