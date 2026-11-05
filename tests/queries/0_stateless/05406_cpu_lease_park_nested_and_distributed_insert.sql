-- Tags: no-parallel, shard
-- no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time.

DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_heavy;
DROP VIEW IF EXISTS v_agg;
DROP WORKLOAD IF EXISTS 05406_capped;
DROP WORKLOAD IF EXISTS 05406_wl;
DROP WORKLOAD IF EXISTS 05406_all;
DROP RESOURCE IF EXISTS 05406_cpu;
CREATE RESOURCE 05406_cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD 05406_all;
CREATE WORKLOAD 05406_wl IN 05406_all SETTINGS max_concurrent_threads = 1;
CREATE WORKLOAD 05406_capped IN 05406_all SETTINGS max_cpus = 0.05, max_burst_cpu_seconds = 0.01;

CREATE TABLE t_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_heavy (x UInt64, y UInt64 MATERIALIZED cityHash64(arrayMap(i -> cityHash64(x, i), range(10)))) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_local, x);

INSERT INTO FUNCTION file(currentDatabase() || '_05406.csv', 'CSV', 'x UInt64') SELECT number FROM numbers(300000)
SETTINGS engine_file_truncate_on_insert = 1;

-- Each query runs in a workload with one CPU slot. A thread that waits for another pipeline of the query (the shard,
-- a writing job, the next packet) must give the slot back, and a nested pipeline must not take a second one.

-- The writing job sends one large block and waits for the remote shard to finish the insert
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_heavy) SELECT number FROM numbers(1000000)
SETTINGS workload = '05406_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, async_insert = 0,
    max_block_size = 65409, max_execution_time = 60, prefer_localhost_replica = 0,
    min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 0, log_queries = 1, log_comment = '05406_q1';

-- The initiator waits for the local writing job, which is idle between the blocks
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), t_local)
SELECT cityHash64(arrayMap(i -> cityHash64(number, i), range(20))) FROM numbers(600000)
SETTINGS workload = '05406_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, async_insert = 0,
    max_block_size = 65409, max_execution_time = 60, prefer_localhost_replica = 1,
    min_insert_block_size_rows = 262144, min_insert_block_size_bytes = 0, log_queries = 1, log_comment = '05406_q2';

-- The initiator waits for a local and a remote writing job
INSERT INTO t_dist SELECT cityHash64(arrayMap(i -> cityHash64(number, i), range(20))) FROM numbers(450000)
SETTINGS workload = '05406_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, async_insert = 0,
    max_block_size = 65409, max_execution_time = 60, prefer_localhost_replica = 1, distributed_foreground_insert = 1,
    log_queries = 1, log_comment = '05406_q3';

-- The local shard is written by a nested pipeline on the inserting thread
INSERT INTO t_dist SELECT cityHash64(arrayMap(i -> cityHash64(number, i), range(20))) FROM numbers(1200000)
SETTINGS workload = '05406_wl', max_threads = 1, max_insert_threads = 1, use_concurrency_control = 1, async_insert = 0,
    max_block_size = 65409, max_execution_time = 60, prefer_localhost_replica = 1, distributed_foreground_insert = 0,
    log_queries = 1, log_comment = '05406_q4';
SYSTEM FLUSH DISTRIBUTED t_dist;

-- The file is read by a nested pipeline on the reading thread
SELECT sum(cityHash64(arrayMap(i -> cityHash64(x, i), range(20)))) FROM file(currentDatabase() || '_05406.csv', 'CSV', 'x UInt64')
SETTINGS workload = '05406_wl', max_threads = 1, use_concurrency_control = 1, max_block_size = 65409, max_execution_time = 60,
    input_format_parallel_parsing = 0, log_queries = 1, log_comment = '05406_q5';

-- GLOBAL IN / GLOBAL JOIN fill their external table by a nested pipeline on the planning thread
-- The initiator counts the subquery rows twice (its read and the shard's read of the external table): no row limit.
SELECT count() FROM remote('127.0.0.2', numbers(100000)) WHERE number GLOBAL IN (SELECT number FROM numbers(15000000))
SETTINGS workload = '05406_wl', max_threads = 1, use_concurrency_control = 1, max_block_size = 65409, max_execution_time = 60,
    enable_parallel_replicas = 0, max_rows_to_read = 0, log_queries = 1, log_comment = '05406_q6';
SELECT count() FROM remote('127.0.0.2', numbers(100000)) AS l GLOBAL JOIN (SELECT number FROM numbers(15000000)) AS r USING (number)
SETTINGS workload = '05406_wl', max_threads = 1, use_concurrency_control = 1, max_block_size = 65409, max_execution_time = 60,
    enable_parallel_replicas = 0, max_rows_to_read = 0, log_queries = 1, log_comment = '05406_q7';

-- A long aggregation in a nested pipeline renews the CPU slot of its thread, so the CPU limit preempts it.
-- It is stopped after 1 s: the CPU limit is far below what any build gets in that time.
-- No JIT: compiling the expressions can take that second before the aggregation starts.
CREATE VIEW v_agg AS SELECT sum(cityHash64(number)) AS s FROM numbers(100000000000);
SELECT * FROM loop(v_agg) LIMIT 1
SETTINGS workload = '05406_capped', max_threads = 1, use_concurrency_control = 1, max_block_size = 65409, max_execution_time = 1,
    timeout_overflow_mode = 'break', compile_expressions = 0, compile_aggregate_expressions = 0, max_rows_to_read = 0,
    log_queries = 1, log_comment = '05406_q8'
FORMAT Null;

SELECT count() FROM t_heavy;
SELECT count() FROM t_local;

SYSTEM FLUSH LOGS query_log;

-- The waiting threads parked their CPU slots
SELECT log_comment, ProfileEvents['ConcurrencyControlParks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment IN ('05406_q1', '05406_q2', '05406_q3')
    AND type = 'QueryFinish' AND is_initial_query
ORDER BY log_comment;

-- The nested pipelines ran on the CPU slot of their thread
SELECT log_comment, ProfileEvents['ConcurrencyControlSlotsAcquired']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment IN ('05406_q4', '05406_q5')
    AND type = 'QueryFinish' AND is_initial_query
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['ConcurrencyControlSlotsAcquired'], ProfileEvents['ConcurrencyControlPreemptions'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05406_q8'
    AND type = 'QueryFinish' AND is_initial_query;

DROP TABLE t_dist;
DROP TABLE t_local;
DROP TABLE t_heavy;
DROP VIEW v_agg;
DROP WORKLOAD 05406_capped;
DROP WORKLOAD 05406_wl;
DROP WORKLOAD 05406_all;
DROP RESOURCE 05406_cpu;
