#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree, no-replicated-database, no-async-insert
# Tag no-shared-merge-tree: SharedMergeTree has no insert quorum.
# Tag no-replicated-database: three explicit replicas share one ZooKeeper path.
# Tag no-async-insert: a sequential quorum insert cannot be asynchronous.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUORUM_SETTINGS="SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_keeper_fault_injection_probability = 0"

for r in r1 r2 r3; do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE dst_$r (part_key Int64, id Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', '$r') PARTITION BY part_key ORDER BY id"
done

$CLICKHOUSE_CLIENT -q "INSERT INTO dst_r1 $QUORUM_SETTINGS VALUES (1, 1)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA dst_r3"

# DROP PARTITION waits for r3, which does not execute its queue yet.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES dst_r3"
$CLICKHOUSE_CLIENT -q "ALTER TABLE dst_r1 DROP PARTITION 1 SETTINGS alter_sync = 2" &
drop_pid=$!

for _ in {1..600}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'dst_r1' AND active")" = 0 ] && break
    sleep 0.1
done

# This insert reaches quorum on r1 and r2 during the wait and is not dropped.
$CLICKHOUSE_CLIENT -q "INSERT INTO dst_r1 $QUORUM_SETTINGS VALUES (1, 2)"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP FETCHES dst_r3"
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES dst_r3"
wait $drop_pid

$CLICKHOUSE_CLIENT -q "SELECT * FROM dst_r1 ORDER BY ALL SETTINGS select_sequential_consistency = 1"
# r3 does not have the part inserted with quorum.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dst_r3 SETTINGS select_sequential_consistency = 1" 2>&1 | grep -o -m1 'REPLICA_IS_NOT_IN_QUORUM'

$CLICKHOUSE_CLIENT -q "SYSTEM START FETCHES dst_r3"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA dst_r3"
$CLICKHOUSE_CLIENT -q "SELECT * FROM dst_r3 ORDER BY ALL SETTINGS select_sequential_consistency = 1"

for r in r1 r2 r3; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE dst_$r"
done
