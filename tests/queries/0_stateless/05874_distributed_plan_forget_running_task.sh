#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables a failpoint.
# no-parallel: the failpoint and the query controller metric are server-wide.
#
# A task that is still running when the coordinator stops waiting for it is forgotten anyway: the worker drops it when it ends,
# so it does not keep its query's controller (and `max_queries` slot) until the worker restarts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_forget_running_task"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_forget_running (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_forget_running SELECT number FROM numbers(100000)"

CONTROLLERS_BEFORE=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'StatelessWorkerQueryControllers'")

# The failing reader frees its resources for 5 s, longer than the coordinator waits for it after the cancel.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT distributed_plan_delay_failed_task_release"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT x, count() FROM t_forget_running WHERE NOT throwIf(x = 777, 'reading task fails')
    GROUP BY x FORMAT Null
    SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0,
    distributed_plan_fallback_to_local_execution = 0" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT distributed_plan_delay_failed_task_release"

# The client gets the error before the teardown ends, so wait for the teardown's lines.
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE"
UNIQUE_ID=""
stopped_waiting=0
kept_running=0
for _ in {1..300}
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    [[ -z $UNIQUE_ID ]] && UNIQUE_ID=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\S+)') FROM system.text_log
        WHERE $RECENT AND query_id = '$QUERY_ID' AND message LIKE 'Starting distributed query, unique id: %'")
    stopped_waiting=$($CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.text_log
        WHERE $RECENT AND query_id = '$QUERY_ID' AND message LIKE '%did not reach a terminal state after cancellation%'")
    # The worker knows the tasks by `<unique id>::<task>` and keeps the forgotten running one until it ends.
    [[ -n $UNIQUE_ID ]] && kept_running=$($CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.text_log
        WHERE $RECENT AND position(message, '$UNIQUE_ID') > 0 AND message LIKE 'Task % is forgotten while running%'")
    [[ $stopped_waiting == 1 && $kept_running == 1 ]] && break
    sleep 0.1
done
echo "coordinator stopped waiting for a task: $stopped_waiting"
echo "worker kept the running task until it ended: $kept_running"

# The held task ends about 5 s after it failed; earlier tests can leave controllers, so compare with the value before.
for _ in {1..200}
do
    controllers=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'StatelessWorkerQueryControllers'")
    (( controllers <= CONTROLLERS_BEFORE )) && break
    sleep 0.1
done
echo "query controllers left: $(( controllers > CONTROLLERS_BEFORE ? controllers - CONTROLLERS_BEFORE : 0 ))"
