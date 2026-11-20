#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration.
# no-parallel: the failpoints hold, break and fail the task dispatch of every distributed query on the server.
#
# A batch whose send fails after its worker's control connection was lost: the loss has already dropped the batch's
# record, so the failure must not remove another one. The first failpoint holds the query thread between recording its
# first batch and sending it, the second breaks the control connections meanwhile, and the third then fails the send.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PAUSE=distributed_plan_pause_after_queueing_start_tasks
FAIL=distributed_plan_fail_sending_start_tasks
BREAK=worker_control_loop_break_connections
OUTPUT="${CLICKHOUSE_TMP}/05837_query.out"

function cleanup()
{
    for failpoint in $PAUSE $FAIL $BREAK
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint" 2>/dev/null
    done
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

CONNECTIONS="(SELECT value FROM system.metrics WHERE metric = 'DistributedPlanControlConnections')"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_send_failure (k UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_send_failure SELECT number FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAIL"
$CLICKHOUSE_CLIENT --query "SELECT sum(k) FROM t_send_failure SETTINGS make_distributed_plan = 1,
    distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_default_reader_bucket_count = 4,
    enable_parallel_replicas = 0, max_rows_to_group_by = 0" > "$OUTPUT" 2>&1 &
CLIENT_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $PAUSE PAUSE"
# The first batch opened the connection to its worker before the failpoint held it.
wait_for "SELECT $CONNECTIONS = 1"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $BREAK"
wait_for "SELECT $CONNECTIONS = 0"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $BREAK"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $PAUSE"
wait "$CLIENT_PID"

echo "query failed: $(grep -q 'DB::Exception' "$OUTPUT" && echo 1 || echo 0)"
echo "server alive: $($CLICKHOUSE_CLIENT --query 'SELECT 1')"
rm -f "$OUTPUT"
