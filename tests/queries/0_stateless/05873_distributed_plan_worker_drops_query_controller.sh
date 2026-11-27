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
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_drops_controller (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_drops_controller SELECT number % 100, number FROM numbers(10000)"

# A worker fails right after registering a task of a batch: it does not keep that task, so the teardown does not wait for it.
QUERY_ID="${CLICKHOUSE_DATABASE}_registration_error"
CONTROLLERS_BEFORE=$(query_controllers)
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_fail_after_registering_task"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_drops_controller GROUP BY k)
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2, max_execution_time = 60" 2>&1 \
    | grep -o -m1 "Injected failure after registering task"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_fail_after_registering_task"
# The controller is dropped after the teardown's forget, which comes after its warning about a task that does not end.
query_controllers_left "$CONTROLLERS_BEFORE"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "SELECT 'teardown waited for the task', count() > 0 FROM $TEXT_LOG AND query_id = '$QUERY_ID'
    AND message LIKE '%did not reach a terminal state after cancellation%'"
