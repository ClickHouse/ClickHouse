#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree
# Tag zookeeper: a CSN log entry is a znode under <transaction_log.zookeeper_path>/log.
# Tag no-parallel: the transaction log is system-global - every transaction of the server commits
#   into the one log that this test appends an entry to by hand.
# Tag no-shared-merge-tree: a public build ignores the sub-lists, but in a SharedMergeTree build they
#   name the tables whose changes a later SYSTEM SYNC TRANSACTION LOG waits to become visible, and the
#   hand-written entry names tables that do not exist, so that wait could never end.

# Test: Tx::CSNEntryData::deserialize reads the labelled sub-lists that follow the cross_replica_id
# of a table line of a CSN log entry: P=<zookeeper path>, A=<added row names>, R=<removed row names>,
# comma separated, in any order, one table line per affected table, each line ending at '\n' or EOF.
# Source of the expected format: the doc comment of CSNEntryData in
# src/Interpreters/TransactionPayloads.h ("A table line is `cross_replica_id`, then P=`zk_path`,
# A=added parts, R=removed parts ... Values must not contain a space, comma or newline - the parser
# splits on those") and the comment of parseAffectedRowLists in
# src/Interpreters/TransactionPayloads.cpp ("in any order, stopping at '\n' or EOF").
#
# A public build never writes the sub-lists itself - getAffectedSMTTables is CLICKHOUSE_CLOUD only -
# so the entry is written by hand with INSERT INTO system.zookeeper, the way
# 05385_transaction_invalid_tid_stored_in_keeper.sh writes an invalidation record. The entry carries
# the empty TID, which processCSNLogs parses and then skips, so it leaves no TID -> CSN mapping and
# no per-table state behind. What stays observable is the snapshot: it reaches the entry's CSN only
# once deserialize accepted the whole payload, sub-lists included. A rejected payload throws out of
# the thread that loads new entries, which then only retries, and the snapshot stays behind.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOG_PATH="/test/clickhouse/txn/log"
POLL_INTERVAL=0.1
POLL_TRIES=100
EMPTY_UUID="00000000-0000-0000-0000-000000000000"
# Two table lines: the first with the labels in the documented order, the second with them reordered
# and ending at EOF instead of '\n'. So the pair also pins the line boundary - a parse that ran past
# the newline of the first line would fail the assertChar('\n') in front of the second one.
PAYLOAD="version: 1\n(0, 0, $EMPTY_UUID)\nreplica_id: $EMPTY_UUID\nsmt_count: 2"
PAYLOAD="$PAYLOAD\n6442450944 P=/zk/path/one A=row_a1,row_a2 R=row_r1"
PAYLOAD="$PAYLOAD\n6442450945 R=row_r2 A=row_a3,row_a4 P=/zk/path/two"

# Creates /log with its reserved entries and starts the thread that absorbs the new ones.
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC TRANSACTION LOG"

# A free CSN above both ends of the log: above the largest name that /log still holds, and above the
# snapshot this server has already loaded. The second half is what makes a rerun on the same server
# deserialize the payload again - cleanup removes the node but leaves last_loaded_csn behind, and
# loadNewEntries skips every name at or below it, so reusing max(/log) + 1 would let a later run pass
# without parsing anything. Keeper raises the sequence counter of the parent on every create, so a
# name appended by hand here does not take the name of the next real commit. Gaps are fine:
# loadNewEntries orders and cuts the child list by CSN, it does not require consecutive names.
CSN=$($CLICKHOUSE_CLIENT -q "SELECT greatest(
    (SELECT max(toUInt64(substring(name, 5))) FROM system.zookeeper WHERE path = '$LOG_PATH'),
    transactionLatestSnapshot()) + 1")
NODE=$(printf 'csn-%010d' "$CSN")

# Pins the premise of the assertions below: the snapshot is still behind the crafted CSN, so the
# only way it can reach it is through deserialize accepting this entry.
$CLICKHOUSE_CLIENT -q "SELECT 'snapshot_behind_crafted_csn', transactionLatestSnapshot() < $CSN"

# The entry is system-global, so it is removed again on every exit path, before the assertions can
# fail. Should deserialize ever reject the payload - the regression this test is here to catch - the
# thread that loads new entries would retry that same entry forever and every later SYSTEM SYNC
# TRANSACTION LOG on this server would time out. system.zookeeper only writes, so the node is
# removed through Keeper, the way 05229_fetch_entry_for_local_part_missing_in_zookeeper.sh removes a
# part node by hand. Registered before the insert, so an interrupt in between is covered too.
cleanup() {
    $CLICKHOUSE_KEEPER_CLIENT -q "rm '$LOG_PATH/$NODE'" > /dev/null 2>&1 || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q \
    "INSERT INTO system.zookeeper (path, name, value) VALUES ('$LOG_PATH', '$NODE', '$PAYLOAD')"

# Bounded: the create fires the watch that the loading thread keeps on /log, so one iteration is
# the normal case. The bound is what turns a rejected payload into a failure instead of a hang.
for ((i = 0; i < POLL_TRIES; i++)); do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot() >= $CSN")" = "1" ] && break
    sleep "$POLL_INTERVAL"
done

$CLICKHOUSE_CLIENT -q "
SELECT 'crafted_entry_in_the_log', count() FROM system.zookeeper
    WHERE path = '$LOG_PATH' AND name = '$NODE';
SELECT 'log_absorbed_the_crafted_entry', transactionLatestSnapshot() >= $CSN;"
