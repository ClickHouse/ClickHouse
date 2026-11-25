#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: the failpoint is hit by the cleanup of every ReplicatedMergeTree table on the server.
# no-replicated-database: `SYSTEM RESTART REPLICA` is used on a single local table.
# no-shared-merge-tree: the test is about the cleanup thread of ReplicatedMergeTree.

# Shutdown must not wait for the cleanup thread to remove a whole batch of old parts from the filesystem.
# The failpoint pauses before each part. Once the shutdown has started, resuming the cleanup once must be
# enough: if it kept removing the batch, it would pause again on the next part and block the restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=rmt_cleanup_pause_before_removing_part_from_filesystem

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"' EXIT

# 100: serial removal, 0: concurrent removal.
for threshold in 100 0; do
    table="t_$threshold"

    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $table (x UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$table', 'r1') ORDER BY x
        SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0,
            concurrent_part_removal_threshold = $threshold;
        SYSTEM STOP MERGES $table;
    "
    for i in {0..9}; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES ($i)"
    done

    $CLICKHOUSE_CLIENT -q "
        SYSTEM ENABLE FAILPOINT $FAILPOINT;
        SYSTEM START MERGES $table;
        OPTIMIZE TABLE $table FINAL;
    "

    # The old parts are already removed from ZooKeeper, and the cleanup is about to remove the first one from the filesystem.
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

    $CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA $table" &
    restart_pid=$!

    # The table becomes readonly after the shutdown flag is set.
    while clickhouse_test_time_left && [[ $($CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = '$table'") != 1 ]]; do
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

    for _ in {1..600}; do
        kill -0 $restart_pid 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 $restart_pid 2>/dev/null; then
        echo "SYSTEM RESTART REPLICA is blocked by the removal of old parts"
    fi

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    wait $restart_pid

    while clickhouse_test_time_left && [[ $($CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = '$table'") != 0 ]]; do
        sleep 0.1
    done

    # The parts skipped on shutdown are removed after the restart.
    while clickhouse_test_time_left && [[ $($CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS part_log;
        SELECT count() FROM system.part_log
        WHERE database = currentDatabase() AND table = '$table' AND event_type = 'RemovePart'") != 10 ]]; do
        sleep 0.5
    done

    $CLICKHOUSE_CLIENT -q "
        SELECT count(), sum(x) FROM $table;
        SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$table/replicas/r1/parts';
        SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = '$table';
    "
done
