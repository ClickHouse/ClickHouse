#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration.
# no-parallel: the failpoints drop the control connections of every distributed query on the server.
#
# A control connection lost while its query runs is reconnected, and the worker attaches the new connection to the tasks
# of the lost one, so the query returns its result. One failpoint drops the initiator's end of every control connection
# once, the other the worker's end of the next connection that asks for statuses.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUTPUT="${CLICKHOUSE_TMP}/05839_query.out"

function cleanup()
{
    for failpoint in worker_control_loop_drop_connections stateless_worker_drop_control_connection
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint" 2>/dev/null
    done
    rm -f "$OUTPUT"
}
trap cleanup EXIT

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

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_control_reconnect (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_control_reconnect SELECT number % 100, number FROM numbers(100000)"

SETTINGS="make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0, max_threads = 1,
    max_block_size = 1000, function_sleep_max_microseconds_per_block = 0, max_execution_time = 300"

# Runs the query `$1`, whose 4 readers sleep 10 s in all, and enables the failpoint `$2` once its tasks run.
function run_with_drop()
{
    local query_id="${CLICKHOUSE_DATABASE}_$1"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c
        FROM t_control_reconnect WHERE sleepEachRow(0.0001) = 0 GROUP BY k) SETTINGS $SETTINGS" > "$OUTPUT" 2>&1 &
    local client_pid=$!
    wait_for "SELECT countIf(query = 'main') = 1 AND countIf(query LIKE 'stage_0_%') > 0 FROM system.processes
        WHERE initial_query_id = '$query_id' AND query_id != '$query_id'"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $2"
    wait "$client_pid"
    echo "$1: $(head -c 300 "$OUTPUT")"
}

run_with_drop initiator_drop worker_control_loop_drop_connections
run_with_drop worker_drop stateless_worker_drop_control_connection

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND"
for name in initiator_drop worker_drop
do
    query_id="${CLICKHOUSE_DATABASE}_${name}"
    # The worker knows the query by `<unique id>`, the unique id of its distributed plan.
    unique_id=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\\\S+)') FROM system.text_log
        WHERE $RECENT AND query_id = '$query_id' AND message LIKE 'Starting distributed query, unique id: %'")
    $CLICKHOUSE_CLIENT --query "SELECT '$name', 'reconnected', ProfileEvents['DistributedPlanControlReconnects'] > 0 FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
    $CLICKHOUSE_CLIENT --query "SELECT '$name', 'attached', count() > 0 FROM system.text_log WHERE $RECENT
        AND message LIKE 'Attached the control connection of $unique_id %'"
done
