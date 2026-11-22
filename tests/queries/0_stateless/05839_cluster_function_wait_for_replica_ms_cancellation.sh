#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The second replica of `test_unavailable_shard` is unreachable and never asks for tasks, so the other one waits for it.

FILES="${CLICKHOUSE_DATABASE}_05839"
SOURCE="s3Cluster('test_unavailable_shard', s3_conn, filename = '${FILES}/*.csv', format = CSV, structure = 'x UInt64')"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${FILES}/{_partition_id}.csv', format = CSV, structure = 'x UInt64')
    PARTITION BY x SELECT number FROM numbers(20) SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    SELECT count() FROM ${SOURCE} SETTINGS cluster_function_wait_for_replica_ms = 1000, log_comment = '05839_wait'"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    SELECT count() FROM ${SOURCE}
    SETTINGS cluster_function_wait_for_replica_ms = 60000, max_execution_time = 2, log_comment = '05839_throw'" 2>/dev/null

${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    SELECT count() FROM ${SOURCE}
    SETTINGS cluster_function_wait_for_replica_ms = 60000, max_execution_time = 2, timeout_overflow_mode = 'break', log_comment = '05839_break'
    FORMAT Null"

# Another branch of the query fails, or satisfies the LIMIT, while the replica waits.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    SELECT sum(x) FROM (SELECT x FROM ${SOURCE} UNION ALL SELECT throwIf(number = 9) + sleepEachRow(0.2) FROM numbers(10))
    SETTINGS cluster_function_wait_for_replica_ms = 60000, max_threads = 2, max_block_size = 1, log_comment = '05839_sibling'" 2>/dev/null

${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
    SELECT x FROM (SELECT x FROM ${SOURCE} WHERE x > 100 UNION ALL SELECT number + sleepEachRow(0.2) FROM numbers(10)) LIMIT 10
    SETTINGS cluster_function_wait_for_replica_ms = 60000, max_threads = 2, max_block_size = 1, log_comment = '05839_limit'
    FORMAT Null"

KILL_QUERY_ID="${CLICKHOUSE_DATABASE}_05839_kill"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query_id "${KILL_QUERY_ID}" --query "
    SELECT count() FROM ${SOURCE} SETTINGS cluster_function_wait_for_replica_ms = 60000, log_comment = '05839_kill'" >/dev/null 2>&1 &

# The files are read in milliseconds, so after a second the query is in the wait.
for _ in {1..300}; do
    [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '${KILL_QUERY_ID}' AND elapsed >= 1") == 1 ]] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${KILL_QUERY_ID}' SYNC FORMAT Null"
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT log_comment, exception_code, if(log_comment = '05839_wait', query_duration_ms >= 1000, query_duration_ms < 30000)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type != 'QueryStart'
        AND log_comment IN ('05839_wait', '05839_throw', '05839_break', '05839_sibling', '05839_limit', '05839_kill')
    ORDER BY log_comment"
