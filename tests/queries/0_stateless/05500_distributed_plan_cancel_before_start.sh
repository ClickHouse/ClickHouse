#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables a failpoint.
# no-parallel: the failpoint holds the task starts of every distributed query on the server.
#
# The coordinator sends the `start_tasks` batches of several workers at once from its sender threads, so a cancel can
# overtake a batch that is on the wire. The cancel names the query, and the worker ignores the query's tasks that arrive later.
# The failpoint holds the worker's start handlers after the body is read, the query is killed meanwhile, and the batches are
# released only after every worker that got a batch has the query's cancel.

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
TASK_LINES="SELECT countIf(message LIKE 'Remembering cancelled unknown task %') AS remembered,
    countIf(message LIKE 'Request URI:%operation=start_tasks%') AS batches_sent, countIf(message LIKE 'Cancelling the % tasks of %') AS cancels,
    sumIf(toUInt64OrZero(extract(message, 'batch of (\\d+) tasks')), message LIKE 'Starting a batch of %') AS arrived,
    countIf(message LIKE 'Ignoring task % its query was cancelled') AS ignored
    FROM $TEXT_LOG AND position(message, '$UNIQUE_ID') > 0"

# Each held batch went to its own worker, and each such worker gets one cancel for the query; batches not sent yet are dropped.
wait_for "SELECT batches_sent > 0 AND cancels = batches_sent FROM ($TASK_LINES)"

# The held batches reach `startTasks` now; the senders skip the other batches as the query is cancelled.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_pause_before_start_task"
wait $CLIENT_PID
wait_for "SELECT arrived > 0 AND ignored = arrived FROM ($TASK_LINES)"

# Tasks run in the worker's default database, so they are found through the initial query.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
echo "-- The query is killed while the worker holds its first batches (one or two of them, so only that some arrived is shown):"
echo "-- each worker that got one gets one cancel for the query and ignores every task of its batch, no task id is remembered, and"
echo "-- no task runs. initial_query_logged shows that the lookup of the tasks finds the query."
$CLICKHOUSE_CLIENT --query "
    SELECT arrived > 0 AS batches_arrived, cancels = batches_sent AS one_cancel_per_worker, ignored = arrived AS all_arrived_ignored,
        remembered,
        (SELECT count() > 0 FROM system.query_log
            WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$QUERY_ID') AS initial_query_logged,
        (SELECT count() FROM system.query_log WHERE $RECENT AND query_id != '$QUERY_ID' AND initial_query_id = '$QUERY_ID') AS ran
    FROM ($TASK_LINES)
    SETTINGS output_format_pretty_row_numbers = 0
    FORMAT PrettyCompactMonoBlock"
