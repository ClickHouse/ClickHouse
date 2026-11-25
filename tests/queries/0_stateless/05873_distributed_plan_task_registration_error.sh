#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the test enables a failpoint.
# no-parallel: the failpoint and the query controller metric are server-wide.
#
# A worker that fails right after registering a task of a batch does not keep that task: the query fails with the injected
# error, the coordinator's teardown finds the task gone instead of waiting for it, and the query's controller is dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_task_registration_error"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_task_registration (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_task_registration SELECT number % 100, number FROM numbers(10000)"

CONTROLLERS_BEFORE=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'StatelessWorkerQueryControllers'")
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT stateless_worker_fail_after_registering_task"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_registration GROUP BY k)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0,
    max_execution_time = 60" 2>&1 | grep -o -m1 "Injected failure after registering task"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT stateless_worker_fail_after_registering_task"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
echo "teardown waits for a task that never runs: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND query_id = '$QUERY_ID'
        AND message LIKE '%did not reach a terminal state after cancellation%'")"
# The other tasks of the query may still be ending; earlier tests can leave controllers, so compare with the value before.
for _ in {1..100}
do
    controllers=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'StatelessWorkerQueryControllers'")
    (( controllers <= CONTROLLERS_BEFORE )) && break
    sleep 0.1
done
echo "query controllers left: $(( controllers > CONTROLLERS_BEFORE ? controllers - CONTROLLERS_BEFORE : 0 ))"
