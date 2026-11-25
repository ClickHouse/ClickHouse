#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide, and with the automatic mode on, a query of any other test can reach it.
# no-fasttest: tests that run alone are kept out of the fast test.

# The plan with parallel replicas takes over the index analysis of the single-node plan. An `UPDATE` applied on
# the fly between the storage snapshots of the two plans makes the skip index analysis of the old snapshot drop
# granules that the read of the new one has to keep, and the count comes out wrong unless both plans share one
# snapshot. The failpoint holds the query between the two snapshots while the `UPDATE` is sent. A query that
# shares one is left out: the other replicas start after the `UPDATE`, and read with snapshots of their own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=auto_parallel_replicas_pause_before_building_replicas_plan

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"
}
trap cleanup EXIT

SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, automatic_parallel_replicas_ignore_thresholds = 1,
    automatic_parallel_replicas_undecided_retry_after_executions = 0, serialize_query_plan = 0, max_threads = 1,
    merge_tree_min_bytes_per_task_for_remote_reading = 1024, max_rows_to_read = 0,
    apply_mutations_on_fly = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0"

# $1: the name of the case, $2: the query, without its outer `SETTINGS`, $3: the value of
# `enable_shared_storage_snapshot_in_query` for the outer query.
function run_case()
{
    local name=$1 query=$2 shared=$3

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_autopr_update;
        CREATE TABLE t_autopr_update (k UInt64, v UInt8, INDEX iv v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
        -- Keep the mutation pending, so that it is applied on the fly.
        SYSTEM STOP MERGES t_autopr_update;
        INSERT INTO t_autopr_update SELECT number, number >= 500000 FROM numbers(1000000);"

    # The first execution collects statistics, the second reaches the decision.
    $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS, enable_shared_storage_snapshot_in_query = $shared,
        log_comment = '${name}_1'" > /dev/null

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"
    $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS, enable_shared_storage_snapshot_in_query = $shared,
        log_comment = '${name}_2'" > "$CLICKHOUSE_TMP/result_$name" &
    local pid=$!

    timeout 120 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_autopr_update UPDATE v = 1 - v WHERE 1 SETTINGS mutations_sync = 0, alter_update_mode = 'heavy'"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    wait $pid

    # The count is 500000 before the update and after it.
    echo -e "$name\t$(cat "$CLICKHOUSE_TMP/result_$name")"
    rm -f "$CLICKHOUSE_TMP/result_$name"
}

# Without a shared snapshot, the plan with parallel replicas is not built.
run_case separate "SELECT count() FROM t_autopr_update WHERE v = 1" 0
# Where only a subquery turns it off, the plan is built, but its read has a snapshot of its own and it is declined.
run_case subquery "SELECT count() FROM (SELECT k FROM t_autopr_update WHERE v = 1
    SETTINGS enable_shared_storage_snapshot_in_query = 0)" 1

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment,
        ProfileEvents['AutoParallelReplicasApplied'] AS applied,
        ProfileEvents['AutoParallelReplicasSkippedDueToSettings'] > 0 AS skipped_due_to_settings,
        ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0 AS not_comparable
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('separate_2', 'subquery_2')
      AND query_id = initial_query_id
    ORDER BY log_comment;
    DROP TABLE t_autopr_update;"
