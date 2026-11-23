#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote executor needs the stateless worker configuration.
#
# The teardown of a killed query waits for its cancelled tasks to end, also when they end one after another, so it forgets them
# instead of leaving them for the worker to reclaim after `control_reattach_timeout_ms`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Polls until the query `$1` returns 1, for at most 60 seconds.
function wait_for()
{
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        [[ $($CLICKHOUSE_CLIENT --query "$1") == 1 ]] && return 0
        sleep 0.05
    done
    echo "Timed out waiting for: $1"
    return 1
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_teardown (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1024"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_teardown SELECT number % 100, number FROM numbers(1000000)"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_teardown FINAL"

SETTINGS="make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 32,
    distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0, max_threads = 1,
    max_block_size = 65409, preferred_block_size_bytes = 100000000, optimize_move_to_prewhere = 0,
    function_sleep_max_microseconds_per_block = 0, max_execution_time = 300"

# Reader i of the 32 sleeps about 30 ms * (i + 1) on its only block, so the cancelled readers end about 60 ms apart on each worker.
query_id="${CLICKHOUSE_DATABASE}_killed"
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_teardown
    PREWHERE intDiv(v, 31250) >= v % 32 WHERE sleepEachRow(0.00003) = 0 GROUP BY k) SETTINGS $SETTINGS" > /dev/null 2>&1 &
client_pid=$!
wait_for "SELECT countIf(query LIKE 'stage_0_%') > 0 FROM system.processes WHERE initial_query_id = '$query_id' AND query_id != '$query_id'"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
wait "$client_pid"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND"
# The worker knows the query by `<unique id>`, the unique id of its distributed plan.
unique_id=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\\\S+)') FROM system.text_log
    WHERE $RECENT AND query_id = '$query_id' AND message LIKE 'Starting distributed query, unique id: %'")
$CLICKHOUSE_CLIENT --query "SELECT 'left to the worker', count() FROM system.text_log WHERE $RECENT AND query_id = '$query_id'
    AND message LIKE '%did not reach a terminal state after cancellation%'"
$CLICKHOUSE_CLIENT --query "SELECT 'kept for a reconnection', count() FROM system.text_log WHERE $RECENT
    AND message LIKE 'Keeping the % tasks of $unique_id %'"
