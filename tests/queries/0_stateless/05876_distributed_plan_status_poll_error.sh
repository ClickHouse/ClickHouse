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

QUERY_ID="${CLICKHOUSE_DATABASE}_status_poll_error"

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

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_status_poll_failure (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_status_poll_failure SELECT number % 100, number FROM numbers(100000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_status_poll_pause_before_snapshot"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_status_poll_snapshot_fault"

# The readers sleep 10 s in all, so the query runs while the first poller is held.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_status_poll_failure
    WHERE sleepEachRow(0.0001) = 0 GROUP BY k)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0,
    max_threads = 1, max_block_size = 1000, function_sleep_max_microseconds_per_block = 0, max_execution_time = 30" 2>&1 \
    | grep -o -m1 -E "FAULT_INJECTED|TIMEOUT_EXCEEDED" > "${CLICKHOUSE_TMP}/status_poll_error_code" &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT distributed_plan_status_poll_pause_before_snapshot PAUSE"
# The other worker's poller fails meanwhile; the query ends once the held poller is released.
wait_for "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
    AND query_id = '$QUERY_ID' AND message LIKE '%Injected failure while copying the tasks to poll%'"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT distributed_plan_status_poll_pause_before_snapshot"

wait $CLIENT_PID
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_status_poll_pause_before_snapshot"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_status_poll_snapshot_fault"
cat "${CLICKHOUSE_TMP}/status_poll_error_code"
