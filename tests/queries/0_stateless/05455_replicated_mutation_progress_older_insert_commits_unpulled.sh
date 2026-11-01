#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: the failpoint is global and pauses the first INSERT that reaches it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT older than a mutation commits after the mutation is created: in one Keeper transaction
# it removes its block lock and publishes its part in the mutation's scope with a replication-log
# entry. A replica that has not pulled that entry does not see this work in either place, so the
# mutation's `progress` there must stay NULL rather than report the known parts only, and stays NULL
# while the pulled part is not fetched yet.
# r2 runs the INSERT, paused after it reserved its block number. r1 runs the mutation and is read.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_unpulled_r1 SYNC;
    DROP TABLE IF EXISTS t_unpulled_r2 SYNC;
    CREATE TABLE t_unpulled_r1 (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_unpulled', 'r1') PARTITION BY p ORDER BY k;
    CREATE TABLE t_unpulled_r2 (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_unpulled', 'r2') PARTITION BY p ORDER BY k;
    SYSTEM STOP MERGES t_unpulled_r2;
    INSERT INTO t_unpulled_r1 SELECT 0, number, 1 FROM numbers(1000);
    INSERT INTO t_unpulled_r1 SELECT 1, number, 2 FROM numbers(1000);
"

# The INSERT on r2 writes one part and holds its block lock in partition 0 until the failpoint is
# disabled. A Keeper fault would make it retry under a new block number, newer than the mutation.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_insert_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_unpulled_r2 SELECT 0, 1000 + number, 1 FROM numbers(1000)
    SETTINGS insert_keeper_fault_injection_probability = 0, async_insert = 0, max_insert_threads = 1" &
inserter=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT rmt_insert_pause_before_commit PAUSE"

# r1 does not fetch the part of that INSERT until the end. The part of partition 1 fails forever,
# so it stays to do with a known size, and the part of partition 0 is rewritten.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP FETCHES t_unpulled_r1"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_unpulled_r1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_unpulled_r1 UPDATE v = v + throwIf(v = 2) WHERE 1 SETTINGS mutations_sync = 0"
pending=""
for _ in {1..300}; do
    pending=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_unpulled_r1' AND parts_to_do = 2")
    [ "$pending" = "1" ] && break
    sleep 0.2
done
[ "$pending" = "1" ] || echo "FAIL: the mutation never reached the queue with both parts"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_unpulled_r1"

failed=""
for _ in {1..300}; do
    failed=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_unpulled_r1' AND parts_to_do = 1 AND latest_fail_reason != ''")
    [ "$failed" = "1" ] && break
    sleep 0.2
done
[ "$failed" = "1" ] || echo "FAIL: the mutation never reached one failing part"

# An attempt on the failing part counts the rows it has read as done until it leaves `system.merges`.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_unpulled_r1"
merging=""
for _ in {1..300}; do
    merging=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_unpulled_r1'")
    [ "$merging" = "0" ] && break
    sleep 0.2
done
[ "$merging" = "0" ] || echo "FAIL: the failing part was still being rewritten"

echo "committing"
$CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL FROM system.mutations WHERE database = currentDatabase() AND table = 't_unpulled_r1'"

# The INSERT commits while r1 cannot pull the replication log.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP PULLING REPLICATION LOG t_unpulled_r1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_insert_pause_before_commit"
wait $inserter

# A pull that was already running when pulling stopped can still bring the entry in, so only `progress` is checked.
echo "committed, not pulled"
$CLICKHOUSE_CLIENT -q "SELECT progress IS NULL FROM system.mutations WHERE database = currentDatabase() AND table = 't_unpulled_r1'"

$CLICKHOUSE_CLIENT -q "SYSTEM START PULLING REPLICATION LOG t_unpulled_r1"
pulled=""
for _ in {1..300}; do
    pulled=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_unpulled_r1' AND parts_to_do = 2")
    [ -n "$pulled" ] && break
    sleep 0.2
done
echo "pulled, not fetched"
echo "${pulled:-FAIL: the part of the INSERT never reached the queue}"

# Once fetched, the part has a size and the older INSERT is gone, so `progress` is known: one of
# three parts of about the same size is done.
$CLICKHOUSE_CLIENT -q "SYSTEM START FETCHES t_unpulled_r1"
fetched=""
for _ in {1..300}; do
    fetched=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress BETWEEN 0.2 AND 0.5 FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_unpulled_r1' AND progress IS NOT NULL")
    [ -n "$fetched" ] && break
    sleep 0.2
done
echo "fetched"
echo "${fetched:-FAIL: progress never became known}"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_unpulled_r1' SYNC FORMAT Null"
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_unpulled_r1 SYNC;
    DROP TABLE t_unpulled_r2 SYNC;
"
