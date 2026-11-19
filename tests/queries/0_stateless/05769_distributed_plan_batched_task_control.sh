#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote executor needs the stateless worker configuration.
#
# The coordinator polls, cancels and forgets the tasks of a query per worker, on the worker's control connection: one
# `GetStatuses` asks about all of a worker's unfinished tasks, and the teardown of a killed query sends one `CancelTasks` per
# worker, whatever the number of tasks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_batched_task_control"

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

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_batched_task_control (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_batched_task_control SELECT number % 100, number FROM numbers(100000)"

SETTINGS="make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0, max_threads = 1,
    max_block_size = 1000, function_sleep_max_microseconds_per_block = 0, max_execution_time = 300"

# A query that ends normally: its tasks are polled and forgotten, none is cancelled.
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_finished" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c
    FROM t_batched_task_control GROUP BY k) SETTINGS $SETTINGS"

# A query killed while its 7 tasks run: the teardown cancels them with one request per worker.
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_killed" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c
    FROM t_batched_task_control WHERE sleepEachRow(0.0001) = 0 GROUP BY k) SETTINGS $SETTINGS" > /dev/null 2>&1 &
CLIENT_PID=$!
wait_for "SELECT countIf(query = 'main') = 1 AND countIf(query LIKE 'stage_0_%') > 0 FROM system.processes
    WHERE initial_query_id = '${QUERY_ID}_killed' AND query_id != initial_query_id"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${QUERY_ID}_killed' SYNC" > /dev/null
wait $CLIENT_PID

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT replaceOne(query_id, '${QUERY_ID}_', '') AS query, tasks > 0, status_requests > 0,
        forget_requests BETWEEN 1 AND tasks, if(query = 'finished', cancel_requests = 0, cancel_requests BETWEEN 1 AND workers),
        workers < tasks
    FROM
    (
        SELECT query_id, ProfileEvents['DistributedPlanRemoteTasks'] AS tasks,
            ProfileEvents['DistributedPlanTaskStatusRequests'] AS status_requests,
            ProfileEvents['DistributedPlanTaskCancelRequests'] AS cancel_requests,
            ProfileEvents['DistributedPlanTaskForgetRequests'] AS forget_requests,
            ProfileEvents['DistributedPlanHostsUsed'] AS workers
        FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND query_id IN ('${QUERY_ID}_finished', '${QUERY_ID}_killed') AND type != 'QueryStart'
    )
    ORDER BY query"
