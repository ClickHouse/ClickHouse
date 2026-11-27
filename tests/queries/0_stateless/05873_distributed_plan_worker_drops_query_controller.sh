#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables failpoints.
# no-parallel: the failpoints and the query controller metric are server-wide.
#
# A worker drops the controller of a query, and so its `max_queries` slot, once the query's tasks are gone, however they end.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_fail_after_registering_task" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_delay_failed_task_release" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_drops_controller (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 1000"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_drops_controller SELECT number % 100, number FROM numbers(100000)"

# A worker fails right after registering a task of a batch: it does not keep that task, so the teardown does not wait for it.
QUERY_ID="${CLICKHOUSE_DATABASE}_registration_error"
CONTROLLERS_BEFORE=$(query_controllers)
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_fail_after_registering_task"
ERROR=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_drops_controller GROUP BY k)
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2, max_execution_time = 60" 2>&1 \
    | grep -o -m1 "FAULT_INJECTED")
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_fail_after_registering_task"
UNIQUE_ID=$(unique_id_of "$QUERY_ID")
# The controller is dropped after the teardown's forget, which comes after its warning about a task that does not end.
LEFT=$(query_controllers_left "$CONTROLLERS_BEFORE")
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
CASES=("('registration_error', '$ERROR', $($CLICKHOUSE_CLIENT --query "
    SELECT countIf(query_id = '$QUERY_ID' AND message LIKE '%did not reach a terminal state after cancellation%') > 0,
        countIf(position(message, '$UNIQUE_ID') > 0 AND message LIKE 'Task % is forgotten while running%') > 0
    FROM $TEXT_LOG" | tr '\t' ','), $LEFT)")

# A failing reader frees its resources for 5 s, longer than the coordinator waits for it after the cancel: the coordinator forgets
# it anyway, and the worker keeps it until it ends.
QUERY_ID="${CLICKHOUSE_DATABASE}_forget_running_task"
CONTROLLERS_BEFORE=$(query_controllers)
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_delay_failed_task_release"
ERROR=$($CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT v, count() FROM t_drops_controller WHERE NOT throwIf(v = 777, 'reading task fails')
    GROUP BY v FORMAT Null SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO")
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_delay_failed_task_release"
UNIQUE_ID=$(unique_id_of "$QUERY_ID")
wait_for "SELECT count() > 0 FROM $TEXT_LOG AND position(message, '$UNIQUE_ID') > 0 AND message LIKE 'Task % is forgotten while running%'"
LEFT=$(query_controllers_left "$CONTROLLERS_BEFORE")
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
CASES+=("('forget_running_task', '$ERROR', $($CLICKHOUSE_CLIENT --query "
    SELECT countIf(query_id = '$QUERY_ID' AND message LIKE '%did not reach a terminal state after cancellation%') > 0,
        countIf(position(message, '$UNIQUE_ID') > 0 AND message LIKE 'Task % is forgotten while running%') > 0
    FROM $TEXT_LOG" | tr '\t' ','), $LEFT)")

echo "-- registration_error: the worker fails right after registering a task of a batch: the query fails with the injected error,"
echo "-- the worker drops the task at once, so the teardown does not wait for it, and no query controller is left."
echo "-- forget_running_task: a failing reader frees its resources for 5 s, longer than the teardown waits for it: the teardown"
echo "-- forgets it anyway, the worker keeps the running task until it ends, and then drops it with the query controller."
$CLICKHOUSE_CLIENT --query "
    SELECT * FROM VALUES('case String, error String, teardown_waited UInt8, worker_kept_running_task UInt8, controllers_left UInt64',
        $(IFS=,; echo "${CASES[*]}"))
    SETTINGS output_format_pretty_row_numbers = 0
    FORMAT PrettyCompactMonoBlock"
