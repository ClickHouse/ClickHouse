#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration.
# no-parallel: the failpoint holds the task starts of every distributed query on the server.
#
# A worker reclaims the tasks of a control connection that closes before the initiator forgot them, once no new connection
# attached to them within `distributed_query.control_reattach_timeout_ms` (10 s in the test config). The failpoint holds the
# worker's `StartTasks` after the frame is read, so neither the start nor the teardown's `CancelTasks` is answered: the killed
# query ends within its bounded teardown, which then closes the connection. Once the failpoint is released, the worker starts
# the batch, and after the reattach timeout cancels its tasks and forgets them. The exchanges are streaming, so no task can
# finish without the initiator reading the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=stateless_worker_pause_before_start_task
QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_before_start"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true
}
trap cleanup EXIT

# Polls until the query `$1` returns 1, for at most 60 seconds.
function wait_for()
{
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        [[ $($CLICKHOUSE_CLIENT --query "$1") == 1 ]] && return 0
        sleep 0.1
    done
    echo "Timed out waiting for: $1"
    return 1
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_cancel_before_start (k UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_cancel_before_start SELECT number FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT sum(k) FROM t_cancel_before_start SETTINGS make_distributed_plan = 1,
    distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_default_reader_bucket_count = 4,
    enable_parallel_replicas = 0, max_rows_to_group_by = 0" > /dev/null 2>&1 &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null

# The failpoint still holds the start, so the client returns only because the teardown stops waiting for the worker.
client_exited=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline ))
do
    if ! kill -0 "$CLIENT_PID" 2>/dev/null; then
        client_exited=1
        break
    fi
    sleep 0.1
done
echo "initiator ended while the worker held the start: $client_exited"
wait "$CLIENT_PID" 2>/dev/null

# The worker knows the tasks by `<unique id>::<task>`, the unique id of the distributed query.
wait_for "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$QUERY_ID'
    AND message LIKE 'Starting distributed query, unique id: %'"
UNIQUE_ID=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\\\S+)') FROM system.text_log
    WHERE event_date >= yesterday() AND query_id = '$QUERY_ID' AND message LIKE 'Starting distributed query, unique id: %'")

# The held batches start now; the worker cancels and forgets their tasks, then drops the query.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait_for "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday()
    AND message = 'Dropped the query controller of $UNIQUE_ID'"

$CLICKHOUSE_CLIENT --query "SELECT 'tasks reclaimed by the worker', count() > 0 FROM system.text_log WHERE event_date >= yesterday()
    AND message LIKE 'Cancelling % tasks of $UNIQUE_ID that the initiator % did not forget before its control connection closed'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
$CLICKHOUSE_CLIENT --query "SELECT 'initial query logged', count() > 0 FROM system.query_log
    WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID'"
# Tasks run in the worker's default database, so they are found through the initial query.
$CLICKHOUSE_CLIENT --query "SELECT 'tasks finished', count() FROM system.query_log WHERE $RECENT AND query_id != '$QUERY_ID'
    AND type = 'QueryFinish'
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID')"
