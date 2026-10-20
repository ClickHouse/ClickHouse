#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: the UPDATE must run on the replica whose fetches are stopped.
# no-shared-merge-tree: the test relies on part fetches between ReplicatedMergeTree replicas.

# A lightweight UPDATE on a partitioned table must also update the rows of a part that the
# replica fetches while it waits for `update_sequential_consistency`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

tables="t_lwu_seq_pruning_a t_lwu_seq_pruning_b"

function cleanup()
{
    for name in $tables; do
        $CLICKHOUSE_CLIENT --query "SYSTEM START FETCHES ${name}_2" 2>/dev/null || true
    done
    wait || true
    for name in $tables; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${name}_1 SYNC; DROP TABLE IF EXISTS ${name}_2 SYNC" 2>/dev/null || true
    done
}
trap cleanup EXIT

function run_case()
{
    local name=$1
    local predicate=$2

    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE ${name}_1 (k UInt64, p UInt8, n Int64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/${name}', '1') PARTITION BY p ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

        CREATE TABLE ${name}_2 (k UInt64, p UInt8, n Int64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/${name}', '2') PARTITION BY p ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

        SET insert_keeper_fault_injection_probability = 0;
        INSERT INTO ${name}_1 VALUES (1, 1, 10);
        SYSTEM SYNC REPLICA ${name}_2;
        SYSTEM STOP FETCHES ${name}_2;
        INSERT INTO ${name}_1 VALUES (2, 1, 20);
    "

    # The second part is not on replica 2 yet, so the UPDATE waits for its fetch.
    $CLICKHOUSE_CLIENT --query "
        UPDATE ${name}_2 SET n = n + 100 WHERE ${predicate}
        SETTINGS enable_lightweight_update = 1, update_sequential_consistency = 1,
            optimize_mutations_with_partition_pruning = 1, enable_shared_storage_snapshot_in_query = 1,
            insert_keeper_fault_injection_probability = 0
    " &

    wait_for_block_allocated "/clickhouse/tables/$CLICKHOUSE_DATABASE/${name}/block_numbers/1" "block-0000000002"

    $CLICKHOUSE_CLIENT --query "SYSTEM START FETCHES ${name}_2"
    wait

    $CLICKHOUSE_CLIENT --query "
        SYSTEM SYNC REPLICA ${name}_1;
        SYSTEM SYNC REPLICA ${name}_2;
        SELECT '${predicate}',
            (SELECT groupArray((k, n)) FROM (SELECT k, n FROM ${name}_1 ORDER BY k)),
            (SELECT groupArray((k, n)) FROM (SELECT k, n FROM ${name}_2 ORDER BY k));
    "
}

run_case t_lwu_seq_pruning_a 'n > 0'
run_case t_lwu_seq_pruning_b 'p = 1'
