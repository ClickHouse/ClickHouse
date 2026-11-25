#!/usr/bin/env bash
# A distributed query that ends, normally or by a kill, ends its tasks on the workers and drops its query controllers there,
# with one request per worker for each of the polls, the cancel and the forget.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_task_teardown (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_task_teardown SELECT number % 100, number FROM numbers(100000)"

RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 300 SECOND"
# Prints the query controllers created and dropped for the distributed query with the unique id `$1`.
function controller_counts()
{
    echo "countIf(message LIKE 'Created the query controller of %') AS controllers_created,
        countIf(message LIKE 'Dropped the query controller of %') AS controllers_dropped
        FROM $TEXT_LOG AND position(message, '$1') > 0"
}

# Persisted exchanges: readers, shuffle aggregators and `main` run in three waves. The query keeps its controller on the worker
# for all of them and drops it at its end.
FINISHED_ID="${CLICKHOUSE_DATABASE}_finished"
FINISHED_QUERY="SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_teardown GROUP BY k)"
EXPECTED=$($CLICKHOUSE_CLIENT --query "$FINISHED_QUERY SETTINGS make_distributed_plan = 0")
FINISHED_RESULT=$($CLICKHOUSE_CLIENT --query_id "$FINISHED_ID" --query "$FINISHED_QUERY
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2")
FINISHED_UID=$(unique_id_of "$FINISHED_ID")
wait_for "SELECT controllers_created > 0 AND controllers_dropped = controllers_created FROM (SELECT $(controller_counts "$FINISHED_UID"))"

# Streaming exchanges: 4 readers sleeping 100 s in all feed 2 aggregation tasks and `main`, so only the kill ends them.
KILLED_ID="${CLICKHOUSE_DATABASE}_killed"
$CLICKHOUSE_CLIENT --query_id "$KILLED_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_teardown
    WHERE sleepEachRow(0.001) = 0 GROUP BY k)
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, $SLOW_READER_SETTINGS, distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2, max_execution_time = 300" > /dev/null 2>&1 &
CLIENT_PID=$!
# A reader can end early (the buckets are uneven), so kill once `main` and a reader run, and judge the tasks that ended from then on.
wait_for_running_tasks "$KILLED_ID"
KILLED_AT=$($CLICKHOUSE_CLIENT --query "SELECT now64(6)")
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$KILLED_ID' SYNC" > /dev/null
wait $CLIENT_PID
KILLED_UID=$(unique_id_of "$KILLED_ID")

# Tasks can outlive the initiator; their rows are in the log once none of them runs. They carry the worker's default database, so
# they are found through the initiator's row.
wait_for "SELECT count() = 0 FROM system.processes WHERE initial_query_id = '$KILLED_ID'"
wait_for "SELECT controllers_created > 0 AND controllers_dropped = controllers_created FROM (SELECT $(controller_counts "$KILLED_UID"))"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "-- finished: a query with persisted exchanges runs its tasks in three waves, keeps one query controller for all of them and"
echo "-- drops it at its end (both worker addresses of the test cluster are this server, so one controller); the result equals local."
echo "-- It polls the workers, cancels nothing and sends one forget_tasks per worker address at its end."
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['DistributedPlanRemoteTasks'] AS tasks, ProfileEvents['DistributedPlanHostsUsed'] AS workers,
        (SELECT controllers_created FROM (SELECT $(controller_counts "$FINISHED_UID"))) AS controllers_created,
        (SELECT controllers_dropped FROM (SELECT $(controller_counts "$FINISHED_UID"))) AS controllers_dropped,
        $([[ $FINISHED_RESULT == "$EXPECTED" ]] && echo 1 || echo 0) AS result_same,
        ProfileEvents['DistributedPlanTaskStatusRequests'] > 0 AS polled,
        ProfileEvents['DistributedPlanTaskCancelRequests'] AS cancel_requests,
        ProfileEvents['DistributedPlanTaskForgetRequests'] AS forget_requests
    FROM system.query_log WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$FINISHED_ID' AND type = 'QueryFinish'
    SETTINGS output_format_pretty_row_numbers = 0
    FORMAT PrettyCompactMonoBlock"
echo "-- killed: the kill ends the query's tasks (some readers end on their own before it, so only that some end after it is"
echo "-- shown), none of the tasks that end after the kill reports a success, and the query controller is dropped. Each worker"
echo "-- address gets at most one cancel_tasks (none if its tasks had all ended) and one forget_tasks."
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['DistributedPlanRemoteTasks'] AS tasks, ProfileEvents['DistributedPlanHostsUsed'] AS workers,
        (SELECT count() > 0 FROM system.query_log WHERE $RECENT AND query_id != '$KILLED_ID' AND type != 'QueryStart'
            AND event_time_microseconds >= '$KILLED_AT' AND initial_query_id = '$KILLED_ID') AS tasks_ended_after_kill,
        (SELECT countIf(type = 'QueryFinish') FROM system.query_log WHERE $RECENT AND query_id != '$KILLED_ID' AND type != 'QueryStart'
            AND event_time_microseconds >= '$KILLED_AT' AND initial_query_id = '$KILLED_ID') AS succeeded_after_kill,
        (SELECT controllers_created FROM (SELECT $(controller_counts "$KILLED_UID"))) AS controllers_created,
        (SELECT controllers_dropped FROM (SELECT $(controller_counts "$KILLED_UID"))) AS controllers_dropped,
        ProfileEvents['DistributedPlanTaskStatusRequests'] > 0 AS polled,
        ProfileEvents['DistributedPlanTaskCancelRequests'] <= ProfileEvents['DistributedPlanHostsUsed'] AS cancels_at_most_one_per_worker,
        ProfileEvents['DistributedPlanTaskForgetRequests'] AS forget_requests
    FROM system.query_log WHERE $RECENT AND current_database = currentDatabase() AND query_id = '$KILLED_ID' AND type != 'QueryStart'
    SETTINGS output_format_pretty_row_numbers = 0
    FORMAT PrettyCompactMonoBlock"
