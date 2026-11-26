#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper
# no-parallel, no-fasttest: the fail points are server-wide and pause the first query that reaches them.

# A query that waits for a mutation (`mutations_sync`) must not take a mutation that is finished,
# and whose entry is already removed by the cleanup thread, for a killed one.
# With `finished_mutations_to_keep = 0` the entry is removed as soon as the mutation is finished,
# so it can happen before the query starts to wait. The fail points make it happen every time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MT_FAILPOINT="storage_shared_merge_tree_mutate_pause_before_wait"
RMT_FAILPOINT="rmt_mutate_pause_before_wait"

function disable_failpoints()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MT_FAILPOINT"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $RMT_FAILPOINT"
}
trap disable_failpoints EXIT

# Waits until the tables have no mutation entries, that is, the mutations are finished and their entries are removed.
function wait_for_no_mutation_entries()
{
    local tables="$1"
    while clickhouse_test_time_left
    do
        if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table IN ($tables)") == "0" ]]
        then
            return
        fi
        sleep 0.2
    done
}

CLEANUP_SETTINGS="finished_mutations_to_keep = 0, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mt;
    DROP TABLE IF EXISTS rmt1;
    DROP TABLE IF EXISTS rmt2;

    CREATE TABLE mt (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS $CLEANUP_SETTINGS;
    CREATE TABLE rmt1 (k UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r1') ORDER BY k SETTINGS $CLEANUP_SETTINGS;
    CREATE TABLE rmt2 (k UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r2') ORDER BY k SETTINGS $CLEANUP_SETTINGS;

    INSERT INTO mt VALUES (1, 1);
    INSERT INTO rmt1 VALUES (1, 1);
    SYSTEM SYNC REPLICA rmt2;
"

# MergeTree
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $MT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE mt UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 1" && echo "mt: the synchronous mutation is finished" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $MT_FAILPOINT PAUSE"
wait_for_no_mutation_entries "'mt'"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT 'mt', x FROM mt"

# ReplicatedMergeTree, waiting for both replicas
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $RMT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE rmt1 UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 2" && echo "rmt: the synchronous mutation is finished" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $RMT_FAILPOINT PAUSE"
wait_for_no_mutation_entries "'rmt1', 'rmt2'"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $RMT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT 'rmt1', x FROM rmt1"
$CLICKHOUSE_CLIENT -q "SELECT 'rmt2', x FROM rmt2"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mt;
    DROP TABLE rmt1;
    DROP TABLE rmt2;
"
