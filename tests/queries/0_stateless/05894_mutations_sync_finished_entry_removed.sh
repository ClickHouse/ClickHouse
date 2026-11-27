#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, zookeeper
# no-parallel, no-fasttest: the fail points are server-wide and pause the first query that reaches them.
# no-replicated-database: a mutation of a MergeTree table goes through the DDL queue of the database there,
# so the query paused on the fail point blocks the queue, and the next mutation cannot be started.

# A query that waits for a mutation (`mutations_sync`) must not take a mutation that is finished,
# and whose entry is already removed by the cleanup thread, for a killed one.
# With `finished_mutations_to_keep = 0` the entry is removed as soon as the mutation is finished,
# so it can happen before the query starts to wait. The fail points make it happen every time.
#
# And the other way around: a killed mutation must be reported as killed
# even if a later mutation is finished and its entry is removed before the query starts to wait.

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

# Waits until the query returns the expected value.
function wait_for_value()
{
    local query="$1"
    local expected="$2"
    while clickhouse_test_time_left
    do
        if [[ $($CLICKHOUSE_CLIENT -q "$query") == "$expected" ]]
        then
            return
        fi
        sleep 0.2
    done
}

# Waits until the tables have the given number of mutation entries in total.
# No entries means that the mutations are finished and their entries are removed.
function wait_for_mutation_entries()
{
    wait_for_value "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table IN ($1)" "$2"
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

echo "MergeTree, the mutation is finished and its entry is removed"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $MT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE mt UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 1" && echo "the query is finished successfully" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $MT_FAILPOINT PAUSE"
wait_for_mutation_entries "'mt'" 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT x FROM mt"

echo "MergeTree, the mutation is killed, the next one is finished and its entry is removed"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES mt"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $MT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE mt UPDATE x = x + 10 WHERE 1 SETTINGS mutations_sync = 1" 2>&1 | grep -q -F "was killed" && echo "the query has failed, because the mutation was killed" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $MT_FAILPOINT PAUSE"
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 'mt'" > /dev/null
wait_for_mutation_entries "'mt'" 0
$CLICKHOUSE_CLIENT -q "
    SYSTEM START MERGES mt;
    ALTER TABLE mt UPDATE x = x + 100 WHERE 1 SETTINGS mutations_sync = 0;
"
wait_for_value "SELECT x FROM mt" 102
wait_for_mutation_entries "'mt'" 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT x FROM mt"

echo "ReplicatedMergeTree, the mutation is finished on both replicas and its entry is removed"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $RMT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE rmt1 UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 2" && echo "the query is finished successfully" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $RMT_FAILPOINT PAUSE"
wait_for_value "SELECT x FROM rmt1" 2
wait_for_value "SELECT x FROM rmt2" 2
wait_for_mutation_entries "'rmt1', 'rmt2'" 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $RMT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT x FROM rmt1"
$CLICKHOUSE_CLIENT -q "SELECT x FROM rmt2"

echo "ReplicatedMergeTree, the mutation is killed, the next one is finished on both replicas and its entry is removed"
$CLICKHOUSE_CLIENT -q "
    SYSTEM STOP MERGES rmt1;
    SYSTEM STOP MERGES rmt2;
"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $RMT_FAILPOINT"
$CLICKHOUSE_CLIENT -q "ALTER TABLE rmt1 UPDATE x = x + 10 WHERE 1 SETTINGS mutations_sync = 2" 2>&1 | grep -q -F "was killed" && echo "the query has failed, because the mutation was killed" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $RMT_FAILPOINT PAUSE"
# The mutation is killed on the other replica, and the replica of the waiting query learns about it from ZooKeeper.
wait_for_mutation_entries "'rmt2'" 1
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 'rmt2'" > /dev/null
wait_for_mutation_entries "'rmt1', 'rmt2'" 0
$CLICKHOUSE_CLIENT -q "
    SYSTEM START MERGES rmt1;
    SYSTEM START MERGES rmt2;
    ALTER TABLE rmt1 UPDATE x = x + 100 WHERE 1 SETTINGS mutations_sync = 0;
"
wait_for_value "SELECT x FROM rmt1" 102
wait_for_value "SELECT x FROM rmt2" 102
wait_for_mutation_entries "'rmt1', 'rmt2'" 0
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $RMT_FAILPOINT"
wait
$CLICKHOUSE_CLIENT -q "SELECT x FROM rmt1"
$CLICKHOUSE_CLIENT -q "SELECT x FROM rmt2"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mt;
    DROP TABLE rmt1;
    DROP TABLE rmt2;
"
