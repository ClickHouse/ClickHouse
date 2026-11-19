#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote executor needs the stateless worker configuration.
#
# The initiator opens one control connection per worker of a distributed plan, whatever the number of its tasks and of its
# dispatches, and pings a connection on which it sent nothing for a second.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_control_connection"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_control_connection (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_control_connection SELECT number % 100, number FROM numbers(100000)"
# One granule, so one of the two readers reads every row and the other none.
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_control_connection_granule (v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_control_connection_granule SELECT number FROM numbers(30)"

SETTINGS="make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    enable_analyzer = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0, max_block_size = 1000,
    function_sleep_max_microseconds_per_block = 0, max_execution_time = 300"

# Seven tasks on the two workers, all sent at once.
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_streaming" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c
    FROM t_control_connection GROUP BY k) SETTINGS $SETTINGS, distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_force_shuffle_aggregation = 1, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_default_shuffle_join_bucket_count = 2"

# The consumer stage is sent after the readers end, on the same connections. The reader with the rows sleeps for 3 s, while
# the connection to the worker of the other reader has nothing to poll.
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_persisted" --query "SELECT count() FROM t_control_connection_granule
    WHERE sleepEachRow(0.1) = 0 SETTINGS $SETTINGS, distributed_plan_force_exchange_kind = 'Persisted',
    distributed_plan_default_reader_bucket_count = 2"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT replaceOne(query_id, '${QUERY_ID}_', '') AS query, workers, tasks > workers, connects = workers,
        if(query = 'persisted', pings > 0, 1)
    FROM
    (
        SELECT query_id, ProfileEvents['DistributedPlanRemoteTasks'] AS tasks,
            ProfileEvents['DistributedPlanHostsUsed'] AS workers,
            ProfileEvents['DistributedPlanControlConnects'] AS connects,
            ProfileEvents['DistributedPlanControlPings'] AS pings
        FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND query_id IN ('${QUERY_ID}_streaming', '${QUERY_ID}_persisted') AND type = 'QueryFinish'
    )
    ORDER BY query"
