#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: the failpoints are global and pause the first INSERT and the first read that reach them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reader A takes its queue snapshot while an older INSERT commits; reader B then sees its fetched part.
# A must not reuse B's check, as its own snapshot misses that part and nothing was rewritten.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_two_readers_r1 SYNC;
    DROP TABLE IF EXISTS t_two_readers_r2 SYNC;
    CREATE TABLE t_two_readers_r1 (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_two_readers', 'r1') PARTITION BY p ORDER BY k;
    CREATE TABLE t_two_readers_r2 (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_two_readers', 'r2') PARTITION BY p ORDER BY k;
    SYSTEM STOP MERGES t_two_readers_r2;
    INSERT INTO t_two_readers_r1 SELECT 0, number, 1 FROM numbers(1000);
    INSERT INTO t_two_readers_r1 SELECT 1, number, 1 FROM numbers(1000);
"

# The INSERT on r2 holds its block lock in partition 0 until its failpoint is disabled.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_insert_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_two_readers_r2 SELECT 0, 1000 + number, 1 FROM numbers(1000)
    SETTINGS insert_keeper_fault_injection_probability = 0, async_insert = 0, max_insert_threads = 1" &
inserter=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT rmt_insert_pause_before_commit PAUSE"

# r1 never rewrites a part, so the mutation has no work done.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_two_readers_r1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_two_readers_r1 UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0"
pending=""
for _ in {1..300}; do
    pending=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_two_readers_r1' AND parts_to_do = 2")
    [ "$pending" = "1" ] && break
    sleep 0.2
done
[ "$pending" = "1" ] || echo "FAIL: the mutation never reached the queue"

# Reader A pauses right after its queue snapshot, which has two parts to do.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_mutations_status_pause_after_queue_snapshot"
$CLICKHOUSE_CLIENT -q "SELECT 'reader A', progress IS NULL, progress > 0 FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_two_readers_r1'" > "${CLICKHOUSE_TMP}/reader_a.txt" &
reader=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT rmt_mutations_status_pause_after_queue_snapshot PAUSE"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_insert_pause_before_commit"
wait $inserter

# Reader B sees the fetched part, with no older INSERT left, and sizes all three parts.
seen=""
for _ in {1..300}; do
    seen=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NOT NULL FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_two_readers_r1'")
    [ "$seen" = "3	1" ] && break
    sleep 0.2
done
echo "reader B ${seen}"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_mutations_status_pause_after_queue_snapshot"
wait $reader
cat "${CLICKHOUSE_TMP}/reader_a.txt"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_two_readers_r1' SYNC FORMAT Null"
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_two_readers_r1 SYNC;
    DROP TABLE t_two_readers_r2 SYNC;
"
