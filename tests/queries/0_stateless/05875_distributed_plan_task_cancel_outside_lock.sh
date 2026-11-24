#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration, and the test enables a failpoint.
# no-parallel: the failpoint is global.
#
# A worker cancels a task without holding the lock of all its tasks, so a slow cancellation (here paused) does not hold up the
# tasks of other queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_task_cancel_outside_lock"

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

SETTINGS="make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0,
    max_threads = 1, max_block_size = 1000, function_sleep_max_microseconds_per_block = 0"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_cancel_outside_lock (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_cancel_outside_lock SELECT number % 100, number FROM numbers(100000)"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_cancel_outside_lock
    WHERE sleepEachRow(0.001) = 0 GROUP BY k) SETTINGS $SETTINGS, max_execution_time = 300" > /dev/null 2>&1 &
CLIENT_PID=$!
wait_for "SELECT countIf(query = 'main') = 1 AND countIf(query LIKE 'stage_0_%') > 0 FROM system.processes
    WHERE initial_query_id = '$QUERY_ID' AND query_id != '$QUERY_ID'"

# The worker's cancellation of the first running task of the killed query pauses; the kill waits for it (the kill's thread can
# run the query's teardown).
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_pause_task_cancel"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null &
KILL_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT stateless_worker_pause_task_cancel PAUSE"

# Another query starts, runs and ends its tasks on the same worker meanwhile.
timeout --kill-after=1 20 $CLICKHOUSE_CLIENT --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_cancel_outside_lock GROUP BY k)
    SETTINGS $SETTINGS, max_execution_time = 20" 2>&1 | head -n1 | cut -f1-2

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT stateless_worker_pause_task_cancel"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_task_cancel"
wait $KILL_PID $CLIENT_PID || true
