#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: a remote distributed plan needs the stateless worker configuration.
# no-parallel: enables global failpoints that would pause or fail other distributed-plan queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR/../shell_config.sh"

set -e

$CLICKHOUSE_CLIENT -q "SELECT value, type FROM system.settings WHERE name = 'distributed_plan_parallel_task_dispatch'"

query_pid=
failpoints=(distributed_plan_pause_first_task_start distributed_plan_pause_second_task_start
    distributed_plan_fail_after_task_start distributed_plan_pause_untracked_task_cleanup)
function cleanup()
{
    for failpoint in "${failpoints[@]}"
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $failpoint" >/dev/null 2>&1 || true
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint" >/dev/null 2>&1 || true
    done
    if [[ -n "$query_pid" ]]
    then
        kill "$query_pid" 2>/dev/null || true
        wait "$query_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0,
    distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    distributed_plan_workers_num = 3"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parallel_task_dispatch"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parallel_task_dispatch (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_parallel_task_dispatch"
for _ in 1 2 3
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_parallel_task_dispatch SELECT number FROM numbers(10)"
done

# Both dispatch modes return the same results, preserve the worker's error, and remain usable after it.
error_file="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_dispatch_error"
for parallel in 0 1
do
    $CLICKHOUSE_CLIENT -q "SELECT x % 3 AS k, sum(x) FROM t_parallel_task_dispatch
        GROUP BY k ORDER BY k SETTINGS $SETTINGS, distributed_plan_parallel_task_dispatch = $parallel"
    if $CLICKHOUSE_CLIENT -q "SELECT intDiv(100, x), count() FROM t_parallel_task_dispatch
        GROUP BY intDiv(100, x) FORMAT NULL SETTINGS $SETTINGS, distributed_plan_parallel_task_dispatch = $parallel" 2>"$error_file"
    then
        echo "failing query unexpectedly succeeded with parallel dispatch = $parallel"
        exit 1
    fi
    if ! grep -q "ILLEGAL_DIVISION" "$error_file"
    then
        cat "$error_file"
        exit 1
    fi
    $CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM t_parallel_task_dispatch
        SETTINGS $SETTINGS, distributed_plan_parallel_task_dispatch = $parallel"
done
rm -f "$error_file"

# Parallel dispatch reaches the second pause point while the first start is paused.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT distributed_plan_pause_first_task_start;
    SYSTEM ENABLE FAILPOINT distributed_plan_pause_second_task_start"
query_id="${CLICKHOUSE_DATABASE}_parallel_start"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT x % 3, sum(x) FROM t_parallel_task_dispatch
    GROUP BY x % 3 FORMAT NULL SETTINGS $SETTINGS, distributed_plan_parallel_task_dispatch = 2" >"$CLICKHOUSE_TMP/$query_id.out" 2>"$CLICKHOUSE_TMP/$query_id.err" &
query_pid=$!
if ! timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT distributed_plan_pause_first_task_start PAUSE" ||
    ! timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT distributed_plan_pause_second_task_start PAUSE"
then
    cat "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
    exit 1
fi
echo "parallel start requests overlapped"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT distributed_plan_pause_first_task_start;
    SYSTEM NOTIFY FAILPOINT distributed_plan_pause_second_task_start;
    SYSTEM DISABLE FAILPOINT distributed_plan_pause_first_task_start;
    SYSTEM DISABLE FAILPOINT distributed_plan_pause_second_task_start"
wait "$query_pid"
query_pid=
rm -f "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"

# A failed start must not hide the root cause already reported by its worker.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT distributed_plan_fail_after_task_start;
    SYSTEM ENABLE FAILPOINT distributed_plan_pause_untracked_task_cleanup"
query_id="${CLICKHOUSE_DATABASE}_failed_start"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT intDiv(100, x) FROM t_parallel_task_dispatch
    FORMAT NULL SETTINGS $SETTINGS, distributed_plan_parallel_task_dispatch = 1, distributed_plan_default_reader_bucket_count = 1, distributed_plan_workers_num = 1" >"$CLICKHOUSE_TMP/$query_id.out" 2>"$CLICKHOUSE_TMP/$query_id.err" &
query_pid=$!
if ! timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT distributed_plan_pause_untracked_task_cleanup PAUSE"
then
    cat "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
    exit 1
fi
for _ in $(seq 1 60)
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    worker_task_id=$($CLICKHOUSE_CLIENT -q "SELECT anyIf(query_id, exception_code = 153) FROM system.query_log
        WHERE initial_query_id = '$query_id' AND query_id != '$query_id'")
    [[ -n "$worker_task_id" ]] && break
    sleep 0.5
done
if [[ -z "$worker_task_id" ]]
then
    echo "worker did not report ILLEGAL_DIVISION before cleanup"
    exit 1
fi
# Wait until the task is terminal, not just until its query_log row is flushed.
$CLICKHOUSE_CURL -sS -X GET "${CLICKHOUSE_URL_INTERSERVER}?endpoint=stateless_worker%2Flocalhost&operation=get_status&task_id=${worker_task_id}&wait_for_ms=30000&compress=false" >/dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT distributed_plan_pause_untracked_task_cleanup;
    SYSTEM DISABLE FAILPOINT distributed_plan_pause_untracked_task_cleanup;
    SYSTEM DISABLE FAILPOINT distributed_plan_fail_after_task_start"
status=0
wait "$query_pid" || status=$?
query_pid=
if [[ "$status" == 0 ]] || ! grep -q "ILLEGAL_DIVISION" "$CLICKHOUSE_TMP/$query_id.err"
then
    cat "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
    exit 1
fi
echo "worker failure propagated"
rm -f "$CLICKHOUSE_TMP/$query_id.out" "$CLICKHOUSE_TMP/$query_id.err"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_parallel_task_dispatch"
