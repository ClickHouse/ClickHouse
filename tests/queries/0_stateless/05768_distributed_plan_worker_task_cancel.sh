#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote executor needs the stateless worker configuration.
#
# A worker cancels a running task through the task's query, as `KILL QUERY` does, so the tasks of a killed distributed query
# end as cancelled instead of reporting a success.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_worker_task_cancel"

# Polls until the query `$1` returns 1, for at most 60 seconds.
function wait_for()
{
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        [[ $($CLICKHOUSE_CLIENT --query "$1") == 1 ]] && return 0
        sleep 0.1
    done
    echo "Timed out waiting for: $1"
    return 1
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_worker_task_cancel (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_worker_task_cancel SELECT number % 100, number FROM numbers(100000)"

# 4 readers sleeping 10 s in all feed 2 aggregation tasks and `main`.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_worker_task_cancel
    WHERE sleepEachRow(0.0001) = 0 GROUP BY k)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0, max_threads = 1,
    max_block_size = 1000, function_sleep_max_microseconds_per_block = 0, max_execution_time = 300" > /dev/null 2>&1 &
CLIENT_PID=$!

# A reader can end early (the buckets are uneven), so kill once `main` and a reader run, and judge only the tasks that ended later.
wait_for "SELECT countIf(query = 'main') = 1 AND countIf(query LIKE 'stage_0_%') > 0 FROM system.processes
    WHERE initial_query_id = '$QUERY_ID' AND query_id != '$QUERY_ID'"
KILLED_AT=$($CLICKHOUSE_CLIENT --query "SELECT now64(6)")
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
wait $CLIENT_PID

# The tasks end after the initiator; their rows are in the log once none of them runs.
wait_for "SELECT count() = 0 FROM system.processes WHERE initial_query_id = '$QUERY_ID'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Tasks ended after the kill, and none of them reports a success.
# The worker's task rows carry its default database, so they are found through the initiator's row.
$CLICKHOUSE_CLIENT --query "SELECT count() > 0, countIf(type = 'QueryFinish') FROM system.query_log
    WHERE event_date >= yesterday() AND query_id != '$QUERY_ID' AND type != 'QueryStart' AND event_time_microseconds >= '$KILLED_AT'
        AND initial_query_id IN (SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$QUERY_ID')"
