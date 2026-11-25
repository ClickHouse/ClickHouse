#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables failpoints.
# no-parallel: the failpoints are global.
#
# A status poller that fails before it sends its request ends the query with its error, instead of leaving its worker unpolled
# and the query waiting until its time limit. The first poller is held, so the failing one is the last poller scheduled, and
# nothing else reports its failure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

QUERY_ID="${CLICKHOUSE_DATABASE}_status_poll_error"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT distributed_plan_status_poll_pause_before_snapshot" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_status_poll_pause_before_snapshot" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_status_poll_snapshot_fault" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_status_poll_error (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_status_poll_error SELECT number % 100, number FROM numbers(100000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_status_poll_pause_before_snapshot"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_status_poll_snapshot_fault"

# The readers sleep 10 s in all, so the query runs while the first poller is held.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_status_poll_error
    WHERE sleepEachRow(0.0001) = 0 GROUP BY k)
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, $SLOW_READER_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2, max_execution_time = 30" 2>&1 \
    | grep -o -m1 -E "FAULT_INJECTED|TIMEOUT_EXCEEDED" > "${CLICKHOUSE_TMP}/status_poll_error_code" &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT distributed_plan_status_poll_pause_before_snapshot PAUSE"
# The other worker's poller fails meanwhile; the query ends once the held poller is released.
wait_for "SELECT count() > 0 FROM $TEXT_LOG AND query_id = '$QUERY_ID' AND message LIKE '%Injected failure while copying the tasks to poll%'"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT distributed_plan_status_poll_pause_before_snapshot"

wait $CLIENT_PID
cat "${CLICKHOUSE_TMP}/status_poll_error_code"
