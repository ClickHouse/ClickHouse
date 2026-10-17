-- Tags: no-parallel
-- no-parallel: the server allows only one MASTER THREAD / WORKER THREAD resource at a time.

DROP WORKLOAD IF EXISTS 05406_wl;
DROP RESOURCE IF EXISTS 05406_cpu;
CREATE RESOURCE 05406_cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD 05406_wl SETTINGS max_concurrent_threads = 1;

-- The shard runs on this server under the same workload. While the initiator waits for it, its parked threads must
-- give the only slot back, although the query wants two.
SELECT number % 10 AS k, count() FROM remote('127.0.0.2', numbers(20000000)) GROUP BY k ORDER BY k
SETTINGS workload = '05406_wl', max_threads = 2, use_concurrency_control = 1, async_socket_for_remote = 1, max_execution_time = 60, log_comment = '05406_remote', log_queries = 1;

SYSTEM FLUSH LOGS query_log;

-- The query ran through CPU slot parking, not an unlimited allocation
SELECT ProfileEvents['ConcurrencyControlParks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05406_remote'
    AND type = 'QueryFinish' AND is_initial_query;

DROP WORKLOAD 05406_wl;
DROP RESOURCE 05406_cpu;
