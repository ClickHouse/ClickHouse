#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-fasttest: needs Keeper and failpoints
# no-parallel: the failpoints are global, so a copy of this test running at the same time could pause on them

# Two attempts of one DDL query with the same `idempotency_key` on a Replicated database: the statement runs exactly
# once, whatever moment the first attempt is held at (before it is enqueued, before it commits, after it commits but
# before it answers, and while the other attempt is being validated), and an attempt whose entry the database cancelled
# because it did not commit in time fails. An error of the statement itself is not hidden by the key.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

R1="${CLICKHOUSE_DATABASE}_r1"
C1="${CLICKHOUSE_DATABASE}_c1"
ZK="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
FAILPOINTS="database_replicated_idempotency_key_pause_after_first_probe database_replicated_idempotency_key_pause_before_enqueue database_replicated_idempotency_key_pause_before_commit database_replicated_idempotency_key_pause_after_commit"

function cleanup()
{
    for fp in $FAILPOINTS; do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $fp" 2>/dev/null
    done
    wait
    for db in $R1 $C1; do
        $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
    done
}
trap cleanup EXIT

# Prints OK, or the name of the error.
function run()
{
    local out
    if out=$("$@" 2>&1); then
        echo OK
    else
        echo "$out" | grep -oE '\([A-Z][A-Z0-9_]+\)' | tail -1 | tr -d '()'
    fi
}

# keyed <key> <query>
function keyed()
{
    run $CLICKHOUSE_CLIENT --enable_ddl_idempotency_key 1 --idempotency_key "$1" --distributed_ddl_output_mode none -q "$2"
}

function plain()
{
    run $CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "$1"
}

function comment_of()
{
    $CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = '$1' AND name = '$2'"
}

function log_size()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$1/log'"
}

# Waits until the log at $1 has more than $2 entries.
function wait_log_size()
{
    local deadline=$((SECONDS + 60))
    while [ "$(log_size "$1")" -le "$2" ] && [ $SECONDS -lt $deadline ]; do
        sleep 0.1
    done
    [ "$(log_size "$1")" -le "$2" ] && echo "the second attempt did not enter the log"
}

function enable_failpoint()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $1"
}

function release_failpoint()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $1"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $1"
}

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $R1 ENGINE = Replicated('$ZK/r', 's1', 'r1')"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $C1 ENGINE = Replicated('$ZK/c', 's1', 'r1') SETTINGS wait_entry_commited_timeout_sec = 1"
for t in a b; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory COMMENT '$t'" > /dev/null
    plain "CREATE TABLE $C1.$t (x UInt8) ENGINE = Memory COMMENT '$t'" > /dev/null
done
for t in z x2 w2; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory" > /dev/null
done

echo "-- the first attempt is held before it is enqueued"
fp=database_replicated_idempotency_key_pause_before_enqueue
enable_failpoint $fp
keyed k1 "USE $R1; EXCHANGE TABLES a AND b" > "$CLICKHOUSE_TMP/a.txt" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k1 "USE $R1; EXCHANGE TABLES a AND b"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
comment_of "$R1" a

echo "-- the second attempt waits in the log for the first one, which is held before it commits"
fp=database_replicated_idempotency_key_pause_before_commit
enable_failpoint $fp
keyed k2 "USE $R1; EXCHANGE TABLES a AND b" > "$CLICKHOUSE_TMP/a.txt" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
size=$(log_size "$ZK/r")
keyed k2 "USE $R1; EXCHANGE TABLES a AND b" > "$CLICKHOUSE_TMP/b.txt" &
wait_log_size "$ZK/r" "$size"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt" "$CLICKHOUSE_TMP/b.txt"
comment_of "$R1" a

echo "-- the database cancels the entry of the first attempt, which is held before it commits"
enable_failpoint $fp
keyed k3 "USE $C1; EXCHANGE TABLES a AND b" > "$CLICKHOUSE_TMP/a.txt" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k3 "USE $C1; EXCHANGE TABLES a AND b"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
comment_of "$C1" a

echo "-- the first attempt committed, but its answer is held"
fp=database_replicated_idempotency_key_pause_after_commit
enable_failpoint $fp
keyed k4 "USE $R1; EXCHANGE TABLES a AND b" > "$CLICKHOUSE_TMP/a.txt" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k4 "USE $R1; EXCHANGE TABLES a AND b"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
comment_of "$R1" a

echo "-- the second attempt fails its own validation because the first one committed meanwhile"
fp=database_replicated_idempotency_key_pause_after_first_probe
i=0
for query in "DROP TABLE z" "RENAME TABLE x2 TO y2" "ALTER TABLE w2 ADD COLUMN c UInt8"; do
    i=$((i + 1))
    enable_failpoint $fp
    keyed "k5_$i" "USE $R1; $query" > "$CLICKHOUSE_TMP/b.txt" &
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
    keyed "k5_$i" "USE $R1; $query"
    [ "$i" = 3 ] && plain "DROP TABLE $R1.w2"
    release_failpoint $fp
    wait
    cat "$CLICKHOUSE_TMP/b.txt"
done
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '$R1' ORDER BY name"

echo "-- errors of the statement itself"
keyed k6 "DROP TABLE $R1.never_existed"
enable_failpoint $fp
keyed k7 "DROP TABLE $R1.never_existed" > "$CLICKHOUSE_TMP/b.txt" &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k7 "CREATE TABLE $R1.q (x UInt8) ENGINE = Memory"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/b.txt"
