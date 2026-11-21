-- Tags: no-fasttest
-- Tag no-fasttest: needs minio

-- The second replica of `test_unavailable_shard` is unreachable and never asks for tasks, so the other one waits for it.

SET send_logs_level = 'fatal';

INSERT INTO FUNCTION s3(s3_conn, filename = concat(currentDatabase(), '_05839/{_partition_id}.csv'), format = CSV, structure = 'x UInt64')
PARTITION BY x SELECT number FROM numbers(20) SETTINGS s3_truncate_on_insert = 1;

SELECT count() FROM s3Cluster('test_unavailable_shard', s3_conn, filename = concat(currentDatabase(), '_05839/*.csv'), format = CSV, structure = 'x UInt64')
SETTINGS cluster_function_wait_for_replica_ms = 1000, log_comment = '05839_wait';

SELECT count() FROM s3Cluster('test_unavailable_shard', s3_conn, filename = concat(currentDatabase(), '_05839/*.csv'), format = CSV, structure = 'x UInt64')
SETTINGS cluster_function_wait_for_replica_ms = 60000, max_execution_time = 2, log_comment = '05839_throw'; -- { serverError TIMEOUT_EXCEEDED }

SELECT count() FROM s3Cluster('test_unavailable_shard', s3_conn, filename = concat(currentDatabase(), '_05839/*.csv'), format = CSV, structure = 'x UInt64')
SETTINGS cluster_function_wait_for_replica_ms = 60000, max_execution_time = 2, timeout_overflow_mode = 'break', log_comment = '05839_break'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, exception_code, if(log_comment = '05839_wait', query_duration_ms >= 1000, query_duration_ms < 30000)
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type != 'QueryStart'
    AND log_comment IN ('05839_wait', '05839_throw', '05839_break')
ORDER BY log_comment;
