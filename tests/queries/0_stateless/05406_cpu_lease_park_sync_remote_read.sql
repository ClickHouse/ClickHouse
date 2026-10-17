-- Tags: no-parallel
-- no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time.

DROP WORKLOAD IF EXISTS 05406_sync_remote_wl;
DROP RESOURCE IF EXISTS 05406_sync_remote_cpu;
CREATE RESOURCE 05406_sync_remote_cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD 05406_sync_remote_wl SETTINGS max_concurrent_threads = 1;

-- The shard runs on this server under the same workload. While the initiator reads its result synchronously,
-- it must give the only slot back.
SELECT number % 10 AS k, count() FROM remote('127.0.0.2', numbers(20000000)) GROUP BY k ORDER BY k
SETTINGS workload = '05406_sync_remote_wl', max_threads = 1, use_concurrency_control = 1, async_socket_for_remote = 0,
         use_hedged_requests = 1, max_execution_time = 60;

SELECT number % 10 AS k, count() FROM remote('127.0.0.2', numbers(20000000)) GROUP BY k ORDER BY k
SETTINGS workload = '05406_sync_remote_wl', max_threads = 1, use_concurrency_control = 1, async_socket_for_remote = 0,
         use_hedged_requests = 0, max_execution_time = 60;

DROP WORKLOAD 05406_sync_remote_wl;
DROP RESOURCE 05406_sync_remote_cpu;
