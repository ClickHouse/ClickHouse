#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoint is global, and the pause would stop the shipped plan of any other query.
# Tag no-fasttest: a test that has to run alone is kept out of the fast test.

# Under plan-based parallel replicas (`parallel_replicas_plan_based`), a replica can deserialize the shipped plan after
# the table changed its structure. The changed header is a regular error, not a logical one.
# https://github.com/ClickHouse/ClickHouse/issues/132667

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pr_structure_changed;
    CREATE TABLE t_pr_structure_changed (k UInt64, b UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_structure_changed VALUES (1, 1);
"

# Every replica is a remote one, and each stops before it deserializes the plan, so nothing is read until they are
# released, and the query waits for them.
# A positive `parallel_replicas_min_number_of_rows_per_replica` would make the planner estimate the rows to read and,
# for this single-row table, not use parallel replicas at all: no replica would reach the failpoint, and
# `SYSTEM WAIT FAILPOINT ... PAUSE` below would wait forever.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"

$CLICKHOUSE_CLIENT -q "
    SELECT k, b, toTypeName(b) FROM t_pr_structure_changed
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
        parallel_replicas_local_plan = 0, parallel_replicas_min_number_of_rows_per_replica = 0
" 2>&1 | grep -o -m1 -E 'INCOMPATIBLE_COLUMNS|LOGICAL_ERROR' &
select_pid=$!

# The replicas hold the plan of `b UInt32`, and deserialize it only after the column became `String`.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT tcp_handler_pause_before_query_plan_deserialization PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_pr_structure_changed MODIFY COLUMN b String"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT tcp_handler_pause_before_query_plan_deserialization"

wait $select_pid

$CLICKHOUSE_CLIENT -q "DROP TABLE t_pr_structure_changed"
