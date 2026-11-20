#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the remote executor needs the stateless worker configuration.
# no-parallel: the failpoint makes the worker answer the pings of every control connection on the server with an error.
#
# A worker's `Error` is matched to the request it answers: one for a ping of the initiator's control loop fails no batch
# and no query. The reader with the rows sleeps for 3 s, while the connection to the worker of the other reader has
# nothing to poll and is pinged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=stateless_worker_answer_ping_with_error
QUERY_ID="${CLICKHOUSE_DATABASE}_error_for_ping"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# One granule, so one of the two readers reads every row and the other none.
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_error_for_ping (v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_error_for_ping SELECT number FROM numbers(30)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
# The loop warns about each error it gets for a ping.
$CLICKHOUSE_CLIENT --send_logs_level error --query_id "$QUERY_ID" --query "SELECT count() FROM t_error_for_ping WHERE sleepEachRow(0.1) = 0
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    enable_analyzer = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0, max_block_size = 1000,
    function_sleep_max_microseconds_per_block = 0, max_execution_time = 300,
    distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_default_reader_bucket_count = 2"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT 'pinged', ProfileEvents['DistributedPlanControlPings'] > 0 FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'"
