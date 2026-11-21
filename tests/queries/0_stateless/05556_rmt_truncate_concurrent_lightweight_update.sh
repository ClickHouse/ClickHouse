#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: two replicas of one table in one database, and the Keeper path is read directly
# no-shared-merge-tree: the test inspects ReplicatedMergeTree block number locks in Keeper

# TRUNCATE must not deadlock with lightweight updates that hold block numbers in several partitions:
# U0 holds its block numbers while it waits for the replication queue, TRUNCATE waits for U0 and U1 waits for TRUNCATE.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_trunc_lwu"
UPDATE_SETTINGS="enable_lightweight_update = 1, update_sequential_consistency = 1, update_parallel_mode = 'async'"
OUT_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t_trunc_lwu_r1" 2>/dev/null
    wait
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_trunc_lwu_r1 SYNC; DROP TABLE IF EXISTS t_trunc_lwu_r2 SYNC" 2>/dev/null
    rm -f "${OUT_PREFIX}".*.out
}
trap cleanup EXIT

function run_in_background()
{
    local name=$1 query=$2
    { $CLICKHOUSE_CLIENT -q "$query" 2>&1 && echo "OK"; } > "${OUT_PREFIX}.$name.out" &
}

function wait_for_locks()
{
    local value_pattern=$1 expected=$2
    for _ in {1..600}; do
        [[ $($CLICKHOUSE_CLIENT -q "
            SELECT count() FROM system.zookeeper
            WHERE path IN ('$ZK_PATH/block_numbers/0', '$ZK_PATH/block_numbers/1') AND value LIKE '$value_pattern'") -ge $expected ]] && return
        sleep 0.1
    done
    echo "Timeout waiting for $expected block numbers with value '$value_pattern'"
}

for r in r1 r2; do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE t_trunc_lwu_$r (p UInt8, id UInt64, v UInt64, w UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_trunc_lwu', '$r') PARTITION BY p ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
done

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability 0 -q "INSERT INTO t_trunc_lwu_r1 SELECT number % 2, number, 0, 0 FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_trunc_lwu_r2"

# A part that r1 cannot fetch while its queue is stopped, so U0 waits in the replication queue holding its block numbers.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES t_trunc_lwu_r1"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability 0 -q "INSERT INTO t_trunc_lwu_r2 VALUES (0, 1000, 0, 0)"

run_in_background U0 "UPDATE t_trunc_lwu_r1 SET v = 1 WHERE id >= 0 SETTINGS $UPDATE_SETTINGS"
wait_for_locks 'operation: 2%' 2

run_in_background TRUNCATE "TRUNCATE TABLE t_trunc_lwu_r1"
wait_for_locks '%abandonable_lock-other' 1

run_in_background U1 "UPDATE t_trunc_lwu_r1 SET w = 1 WHERE id >= 0 SETTINGS $UPDATE_SETTINGS"
wait_for_locks 'operation: 2%' 4

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES t_trunc_lwu_r1"
wait

for name in U0 TRUNCATE U1; do
    sed "s/^/$name: /" "${OUT_PREFIX}.$name.out"
done

$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_trunc_lwu_r1"
$CLICKHOUSE_CLIENT -q "SELECT 'after truncate', count() FROM t_trunc_lwu_r1"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability 0 -q "INSERT INTO t_trunc_lwu_r1 SELECT number % 2, number, 0, 0 FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "SELECT 'after insert', count(), sum(v), sum(w) FROM t_trunc_lwu_r1 SETTINGS apply_patch_parts = 1"
