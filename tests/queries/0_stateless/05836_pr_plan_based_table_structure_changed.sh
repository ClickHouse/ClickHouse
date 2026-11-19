#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoints are global, and the pause would stop the shipped plan of any other query.

# Under plan-based parallel replicas (`parallel_replicas_plan_based`), a replica can deserialize the shipped plan after
# the table changed its structure: here the replica is cancelled as unused, its query returns, and the replica gets to
# the plan only after the next `ALTER`. The changed header is a regular error, not a logical one.
# https://github.com/ClickHouse/ClickHouse/issues/132667

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pr_structure_changed;
    CREATE TABLE t_pr_structure_changed (k UInt64, b UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_structure_changed VALUES (1, 1);
"

# The remote replica stops before it deserializes the plan, so it cannot ask for marks, and the local replica reads the
# only mark. The local read is slowed down so that the query reaches the remote replica before it is cancelled.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read"

query_id="05836_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT k, b, toTypeName(b) FROM t_pr_structure_changed
    SETTINGS enable_analyzer = 1, enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
        parallel_replicas_local_plan = 1
"

# The query has returned, and the replica is still waiting with the plan of `b UInt32`.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT tcp_handler_pause_before_query_plan_deserialization PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_pr_structure_changed MODIFY COLUMN b String"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT tcp_handler_pause_before_query_plan_deserialization"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"

# The secondary query writes its row to `system.query_log` once it fails.
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    found=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
            AND initial_query_id = '$query_id' AND NOT is_initial_query AND type != 'QueryStart'
    ")
    [ "$found" -ge 1 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT type, errorCodeToName(exception_code) FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
        AND initial_query_id = '$query_id' AND NOT is_initial_query AND type != 'QueryStart'
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_pr_structure_changed"
