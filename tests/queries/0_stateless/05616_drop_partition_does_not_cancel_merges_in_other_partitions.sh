#!/usr/bin/env bash

# `DROP PARTITION` and `DETACH PARTITION` of one partition of a `MergeTree` table must not cancel
# merges and mutations that are running in other partitions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With one row per granule, merges and mutations process one row per block, so `sleepEachRow(0.1)`
# keeps them running for seconds and they notice a cancellation within one block.
for table in t_merge t_mutation; do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $table (p UInt8, x UInt64)
        ENGINE = MergeTree
        PARTITION BY p
        ORDER BY tuple()
        SETTINGS index_granularity = 1, number_of_free_entries_in_pool_to_execute_mutation = 0
    "
done

# A merge in partition 1. The parts have no column `s`, so the merge evaluates its default for every row.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_merge"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_merge SELECT 1, number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_merge SELECT 1, number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_merge SELECT 2, number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_merge ADD COLUMN s UInt8 DEFAULT sleepEachRow(0.1)"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_merge"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_merge PARTITION 1 FINAL" 2>/dev/null &
optimize_pid=$!

# A mutation in partition 1. Partition 2 is inserted after the mutation, so the mutation has nothing to do there.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mutation SELECT 1, number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mutation UPDATE x = x + sleepEachRow(0.1) IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mutation SELECT 2, number FROM numbers(100)"

function wait_until_running()
{
    for _ in {1..600}; do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$1'")" -gt 0 ] && return
        sleep 0.1
    done
    echo "Timed out waiting for a merge or mutation of $1 to start"
}

wait_until_running t_merge
wait_until_running t_mutation

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_merge DROP PARTITION 2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mutation DETACH PARTITION 2"

# The merge and the mutation of partition 1 are still running after the drop and the detach.
$CLICKHOUSE_CLIENT -q "SELECT table, is_mutation FROM system.merges WHERE database = currentDatabase() ORDER BY ALL"

# A cancelled merge or mutation writes its part_log entry before the drop returns.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
$CLICKHOUSE_CLIENT -q "
    SELECT table, event_type, partition_id, errorCodeToName(error)
    FROM system.part_log
    WHERE database = currentDatabase() AND error != 0
    ORDER BY ALL
"
$CLICKHOUSE_CLIENT -q "
    SELECT table, partition_id, sum(rows)
    FROM system.parts
    WHERE database = currentDatabase() AND active AND rows > 0
    GROUP BY ALL
    ORDER BY ALL
"

# Cancel the slow merge and mutation, they are not needed anymore.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_merge"
wait $optimize_pid
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_mutation' SYNC FORMAT Null"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_merge"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mutation"
