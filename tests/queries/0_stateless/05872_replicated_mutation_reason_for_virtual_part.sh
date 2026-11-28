#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: the test creates replicas explicitly.

# A mutation of a `ReplicatedMergeTree` may wait for a part which is not created yet: the result of
# a merge which is still in the replication queue. Such a part is listed in `parts_to_do`, but the
# merge selecting task considers only existing data parts, so no reason was recorded and the mutation
# looked as if it was never scheduled. Now `parts_postpone_reasons` explains what it is waiting for.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS rmt_virtual_r1 SYNC;
        DROP TABLE IF EXISTS rmt_virtual_r2 SYNC;
    " 2>/dev/null || true
}
trap cleanup EXIT

# Disable fault injection: part names are non-deterministic in case of insert retries.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS rmt_virtual_r1 SYNC;
    DROP TABLE IF EXISTS rmt_virtual_r2 SYNC;

    CREATE TABLE rmt_virtual_r1 (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_virtual', 'r1') ORDER BY id;
    CREATE TABLE rmt_virtual_r2 (id UInt64, num UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_virtual', 'r2') ORDER BY id;

    SYSTEM STOP MERGES rmt_virtual_r1;
    INSERT INTO rmt_virtual_r1 VALUES (1, 1);
    INSERT INTO rmt_virtual_r1 VALUES (2, 2);
    SYSTEM SYNC REPLICA rmt_virtual_r2;

    -- The merge is executed by r1, but stays in the queue of r2.
    SYSTEM STOP MERGES rmt_virtual_r2;
    SYSTEM STOP FETCHES rmt_virtual_r2;
    SYSTEM START MERGES rmt_virtual_r1;
    OPTIMIZE TABLE rmt_virtual_r1 FINAL;

    ALTER TABLE rmt_virtual_r1 UPDATE num = num + 1 WHERE 1 SETTINGS mutations_sync = 0;
"

reason_pattern='Part is not created yet, waiting for queue entry queue-% (MERGE_PARTS)%'

for _ in {1..300}; do
    found=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 'rmt_virtual_r2' AND NOT is_done
            AND arrayExists(x -> x LIKE '$reason_pattern', mapValues(parts_postpone_reasons))")
    [[ "$found" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, parts_to_do_names, mapKeys(parts_postpone_reasons),
        arrayAll(x -> x LIKE '$reason_pattern', mapValues(parts_postpone_reasons)), is_done
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_virtual_r2'
    ORDER BY mutation_id;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM START FETCHES rmt_virtual_r2;
    SYSTEM START MERGES rmt_virtual_r2;
"

wait_for_mutation "rmt_virtual_r2" "0000000000"

$CLICKHOUSE_CLIENT --query "
    SYSTEM SYNC REPLICA rmt_virtual_r2;
    SELECT mutation_id, parts_to_do_names, parts_postpone_reasons, is_done
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_virtual_r2'
    ORDER BY mutation_id;
    SELECT id, num FROM rmt_virtual_r2 ORDER BY id;
"
