#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables a failpoint.
# no-parallel: the failpoint is global.
#
# A worker cancels a task without holding the lock of all its tasks, so a slow cancellation (here paused) does not hold up the
# tasks of other queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

QUERY_ID="${CLICKHOUSE_DATABASE}_task_cancel_outside_lock"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT stateless_worker_pause_task_cancel" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_task_cancel" 2>/dev/null || true
}
trap cleanup EXIT

SETTINGS="$DISTRIBUTED_PLAN_SETTINGS, $SLOW_READER_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_default_shuffle_join_bucket_count = 2"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_cancel_outside_lock (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_cancel_outside_lock SELECT number % 100, number FROM numbers(100000)"

SECOND_ID="${CLICKHOUSE_DATABASE}_second_query"
SECOND_QUERY="SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_cancel_outside_lock GROUP BY k)"
EXPECTED=$($CLICKHOUSE_CLIENT --query "$SECOND_QUERY SETTINGS make_distributed_plan = 0")

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_cancel_outside_lock
    WHERE sleepEachRow(0.001) = 0 GROUP BY k) SETTINGS $SETTINGS, max_execution_time = 300" > /dev/null 2>&1 &
CLIENT_PID=$!
wait_for_running_tasks "$QUERY_ID"

# The worker's cancellation of the first running task of the killed query pauses; the kill waits for it (the kill's thread can
# run the query's teardown).
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_pause_task_cancel"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null &
KILL_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT stateless_worker_pause_task_cancel PAUSE"

# Another query starts, runs and ends its tasks on the same worker meanwhile.
SECOND_RESULT=$(timeout --kill-after=1 20 $CLICKHOUSE_CLIENT --query_id "$SECOND_ID" --query "$SECOND_QUERY
    SETTINGS $SETTINGS, max_execution_time = 20" 2>&1)
FIRST_HELD=$($CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'")

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT stateless_worker_pause_task_cancel"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_task_cancel"
wait $KILL_PID $CLIENT_PID || true

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
echo "-- The first query is killed, and the worker's cancellation of one of its tasks is paused, so the query is still running"
echo "-- (first_query_held_by_the_cancel). Meanwhile a second query starts, runs and ends all its tasks on the same worker and"
echo "-- returns the local result: the paused cancellation does not hold the lock of all the worker's tasks. Released, the"
echo "-- cancellation ends the first query as cancelled."
$CLICKHOUSE_CLIENT --query "
    SELECT $FIRST_HELD AS first_query_held_by_the_cancel,
        (SELECT any(ProfileEvents['DistributedPlanRemoteTasks']) FROM system.query_log
            WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$SECOND_ID' AND type = 'QueryFinish') AS second_query_tasks,
        (SELECT count() FROM system.query_log
            WHERE $RECENT AND initial_query_id = '$SECOND_ID' AND query_id != '$SECOND_ID' AND type = 'QueryFinish') AS second_query_tasks_ran,
        $([[ $SECOND_RESULT == "$EXPECTED" ]] && echo 1 || echo 0) AS second_query_result_same,
        (SELECT any(exception_code) = 394 FROM system.query_log
            WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type != 'QueryStart') AS first_query_cancelled
    SETTINGS output_format_pretty_row_numbers = 0
    FORMAT PrettyCompactMonoBlock"
