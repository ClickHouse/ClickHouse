#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest, no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.

# The tasks of a stage are started concurrently. A start that fails while other starts of the stage are in
# flight must report its error, and the tasks that were started must be stopped. The stage has more tasks
# than the starts that run at once (16), so some are still queued.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_partial_task_start;
    CREATE TABLE t_partial_task_start (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
    INSERT INTO t_partial_task_start SELECT number FROM numbers(100000);
"

settings="make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 20, distributed_plan_default_reader_bucket_count = 20,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0, distributed_plan_fallback_to_local_execution = 0"

query_id="${CLICKHOUSE_DATABASE}_partial_task_start_${RANDOM}"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT distributed_plan_task_start_fails_after_send"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT x, count() FROM t_partial_task_start GROUP BY x FORMAT Null SETTINGS $settings" 2>&1 \
    | grep -o -m1 "CANNOT_SCHEDULE_TASK"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT distributed_plan_task_start_fails_after_send"

# The tasks of the failed query, including the starts that completed after the failure, are stopped. The
# consumer stage waits for producers that were never started, so a task left behind would not end on its own.
for _ in {1..600}; do
    left=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE initial_query_id = '$query_id' AND query_id != '$query_id'")
    [ "$left" = 0 ] && break
    sleep 0.1
done
echo "tasks left: $left"

# Without the injected failure the same query reports every group.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT x, count() FROM t_partial_task_start GROUP BY x) SETTINGS $settings"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_partial_task_start"
