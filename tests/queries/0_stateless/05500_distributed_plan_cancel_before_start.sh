#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables a failpoint.
# no-parallel: the failpoint holds the task starts of every distributed query on the server.
#
# The coordinator sends the `start_tasks` batches of several workers at once from its sender threads, so a cancel can
# overtake a batch that is on the wire. The worker remembers the cancelled ids and ignores their late start.
# The failpoint holds the worker's start handlers after the body is read, the query is killed meanwhile, and the batches are
# released only after the worker has remembered every task the query queued.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_before_start"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_before_start_task" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_cancel_before_start (k UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_cancel_before_start SELECT number FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_pause_before_start_task"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT sum(k) FROM t_cancel_before_start
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_task_dispatch_threads = 2, distributed_plan_default_reader_bucket_count = 4" \
    > /dev/null 2>&1 &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT stateless_worker_pause_before_start_task PAUSE"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null

UNIQUE_ID=$(unique_id_of "$QUERY_ID")
TASK_LINES="SELECT countIf(message LIKE 'Queueing task %') AS queued, countIf(message LIKE 'Remembering cancelled unknown task %') AS remembered,
    sumIf(toUInt64OrZero(extract(message, 'batch of (\\d+) tasks')), message LIKE 'Starting a batch of %') AS arrived,
    countIf(message LIKE 'Ignoring the start of task %') AS ignored
    FROM $TEXT_LOG AND position(message, '$UNIQUE_ID') > 0"

# Every queued task is cancelled while unknown to the worker: up to two batches are held there, the rest were not sent.
expect_eventually "every queued task remembered" "SELECT queued > 0 AND remembered = queued FROM ($TASK_LINES)"

# The held batches reach `startTasks` now; the senders skip the other batches as the query is cancelled.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_before_start_task"
wait $CLIENT_PID
expect_eventually "every task that arrived ignored" "SELECT arrived > 0 AND ignored = arrived FROM ($TASK_LINES)"

# No task of the query ran on the worker. Tasks run in the worker's default database, so they are found through the initial query.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
$CLICKHOUSE_CLIENT --query "SELECT 'initial query logged', count() > 0 FROM system.query_log
    WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID'"
$CLICKHOUSE_CLIENT --query "SELECT 'tasks executed', count() FROM system.query_log WHERE $RECENT AND query_id != '$QUERY_ID'
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID')"
