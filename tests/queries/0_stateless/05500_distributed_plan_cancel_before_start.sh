#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration.
# no-parallel: the failpoint holds the task starts of every distributed query on the server.
#
# With `distributed_plan_task_dispatch_threads` > 0 the coordinator sends the starts of a stage from a pool, so a cancel can
# overtake a start that is on the wire. The worker remembers the cancelled id and ignores the late start. The failpoint
# holds the worker's start handlers after the body is read, the query is killed meanwhile, and the starts are released only
# after the worker has remembered every task the query queued.

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
    distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0, distributed_plan_task_dispatch_threads = 2,
    distributed_plan_default_reader_bucket_count = 4, enable_parallel_replicas = 0, max_rows_to_group_by = 0" > /dev/null 2>&1 &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null

# The worker knows the tasks by `<unique id>::<task>`, the unique id of the distributed query (URL-encoded in a request line).
wait_for "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND query_id = '$QUERY_ID'
    AND message LIKE 'Starting distributed query, unique id: %'"
UNIQUE_ID=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\\\S+)') FROM system.text_log
    WHERE event_date >= yesterday() AND query_id = '$QUERY_ID' AND message LIKE 'Starting distributed query, unique id: %'")
TASK_LINES="SELECT countIf(message LIKE 'Queueing task %') AS queued, countIf(message LIKE 'Remembering cancelled unknown task %') AS remembered,
    countIf(message LIKE 'Request URI: %operation=start%') AS starts, countIf(message LIKE 'Ignoring the start of task %') AS ignored
    FROM system.text_log WHERE event_date >= yesterday() AND position(message, '$UNIQUE_ID') > 0"

# Every queued task is cancelled while unknown to the worker: up to two starts are held there, the rest were not sent.
wait_for "SELECT queued > 0 AND remembered = queued FROM ($TASK_LINES)"

# The held starts reach `startTask` now; the senders skip the other tasks as the query is cancelled.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $CLIENT_PID
wait_for "SELECT starts > 0 AND ignored = starts FROM ($TASK_LINES)"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT 'every queued task remembered', remembered = queued, 'every start that arrived ignored', starts > 0 AND ignored = starts
    FROM ($TASK_LINES)"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
$CLICKHOUSE_CLIENT --query "SELECT 'initial query logged', count() > 0 FROM system.query_log
    WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID'"
# No task of the query ran on the worker. Tasks run in the worker's default database, so they are found through the initial query.
$CLICKHOUSE_CLIENT --query "SELECT 'tasks executed', count() FROM system.query_log WHERE $RECENT AND query_id != '$QUERY_ID'
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID')"
