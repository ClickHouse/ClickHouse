#!/usr/bin/env bash
# A query keeps its controller, and so its `max_queries` slot, on a worker until it ends: with persisted exchanges the finished
# tasks of one wave are forgotten only at the end of the query, so the next wave is not admitted as a new query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_persisted_keeps_controller"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_persisted_controller (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_persisted_controller SELECT number % 100, number FROM numbers(100000)"

# Readers, shuffle aggregators and `main` run in three waves.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --query "SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_persisted_controller GROUP BY k)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_force_exchange_kind = 'Persisted', distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2, enable_analyzer = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, enable_cascades_optimizer = 0, max_rows_to_group_by = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
RECENT="event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE"
UNIQUE_ID=$($CLICKHOUSE_CLIENT --query "SELECT extract(any(message), 'unique id: (\\\\S+)') FROM system.text_log
    WHERE $RECENT AND query_id = '$QUERY_ID' AND message LIKE 'Starting distributed query, unique id: %'")
# The test cluster has two worker addresses, so the query needs at most two controllers.
$CLICKHOUSE_CLIENT --query "SELECT 'controllers created at most once per worker', countIf(message LIKE 'Created the query controller of %') BETWEEN 1 AND 2
    FROM system.text_log WHERE $RECENT AND position(message, '$UNIQUE_ID') > 0"
