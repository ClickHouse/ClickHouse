#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the dispatch thread metric is server-wide.
#
# The threads that send a distributed query's task batches go back to the global pool once the batches are sent, instead of
# staying idle until the query ends.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_task_senders_released"

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

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_task_senders (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_task_senders SELECT number % 100, number FROM numbers(100000)"

# Streaming exchanges: every batch is sent at the start, then the readers sleep 30 s in all.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_senders
    WHERE sleepEachRow(0.0003) = 0 GROUP BY k)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Streaming', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0,
    max_threads = 1, max_block_size = 1000, function_sleep_max_microseconds_per_block = 0, max_execution_time = 300" > /dev/null 2>&1 &
CLIENT_PID=$!

wait_for "SELECT countIf(query = 'main') = 1 AND countIf(query LIKE 'stage_0_%') > 0 FROM system.processes
    WHERE initial_query_id = '$QUERY_ID' AND query_id != '$QUERY_ID'"

# The batches are sent by now; sample the metric while the query still runs.
sleep 1
idle_samples=0
for _ in {1..5}
do
    value=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'DistributedPlanTaskDispatchThreads'")
    (( value != 0 )) && idle_samples=$((idle_samples + 1))
    sleep 0.2
done
echo "running: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")"
echo "samples with sender threads: $idle_samples"

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null 2>&1
wait $CLIENT_PID || true
