#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoint is global, and the pause would stop the shipped plan of any other query.
# Tag no-fasttest: a test that has to run alone is kept out of the fast test.

# Under plan-based parallel replicas (`parallel_replicas_plan_based`), a column that only `PREWHERE` or a row policy
# reads can change its type before a replica deserializes the shipped plan. Such a column does not reach the header of
# the read, but the replica must still fail with a regular error, not a logical one.
# https://github.com/ClickHouse/ClickHouse/issues/133438

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROW_POLICY="rp_pr_filter_changed_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS ${ROW_POLICY} ON t_pr_filter_changed"
}
trap cleanup EXIT

SETTINGS="
    enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 0, compile_expressions = 0"

# The replicas hold the plan of `b UInt32`, whose `b + 1` is `UInt64`, and deserialize it only after `b` became `Int32`.
function run_with_alter()
{
    local query=$1
    local setup=$2

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_pr_filter_changed;
        CREATE TABLE t_pr_filter_changed (k UInt64, b UInt32) ENGINE = MergeTree ORDER BY k;
        INSERT INTO t_pr_filter_changed VALUES (1, 1);
    "
    if [ -n "$setup" ]; then
        $CLICKHOUSE_CLIENT -q "$setup"
    fi

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"

    $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS" 2>&1 | grep -o -m1 -E 'INCOMPATIBLE_COLUMNS|LOGICAL_ERROR' &
    local select_pid=$!

    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT tcp_handler_pause_before_query_plan_deserialization PAUSE"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_pr_filter_changed MODIFY COLUMN b Int32"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"

    wait $select_pid

    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS ${ROW_POLICY} ON t_pr_filter_changed"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_pr_filter_changed"
}

echo "PREWHERE"
run_with_alter "SELECT k FROM t_pr_filter_changed PREWHERE b + 1 = 2" ""

echo "row policy"
run_with_alter "SELECT k FROM t_pr_filter_changed" "CREATE ROW POLICY ${ROW_POLICY} ON t_pr_filter_changed USING b + 1 = 2 TO ALL"
