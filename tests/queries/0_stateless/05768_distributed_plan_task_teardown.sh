#!/usr/bin/env bash
# A distributed query that ends, normally or by a kill, ends its tasks on the workers and drops its query controllers there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_task_teardown (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_task_teardown SELECT number % 100, number FROM numbers(100000)"

# Persisted exchanges: readers, shuffle aggregators and `main` run in three waves. The query keeps its controller on each worker
# for all of them (the test cluster has two worker addresses) and drops it at its end.
FINISHED_ID="${CLICKHOUSE_DATABASE}_finished"
$CLICKHOUSE_CLIENT --query_id "$FINISHED_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_teardown GROUP BY k)
    SETTINGS $DISTRIBUTED_PLAN_SETTINGS, distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2"
FINISHED_UID=$(unique_id_of "$FINISHED_ID")
expect_eventually "finished query: one controller per worker, dropped at the end" "SELECT created BETWEEN 1 AND 2 AND dropped = created
    FROM (SELECT countIf(message LIKE 'Created the query controller of %') AS created,
        countIf(message LIKE 'Dropped the query controller of %') AS dropped FROM $TEXT_LOG AND position(message, '$FINISHED_UID') > 0)"

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

# Tasks can outlive the initiator; their rows are in the log once none of them runs. They carry the worker's default database, so
# they are found through the initiator's row.
wait_for "SELECT count() = 0 FROM system.processes WHERE initial_query_id = '$KILLED_ID'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT 'killed query: tasks ended after the kill', count() > 0, 'succeeded', countIf(type = 'QueryFinish')
    FROM system.query_log WHERE event_date >= yesterday() AND query_id != '$KILLED_ID' AND type != 'QueryStart'
        AND event_time_microseconds >= '$KILLED_AT' AND initial_query_id IN (SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$KILLED_ID')"
