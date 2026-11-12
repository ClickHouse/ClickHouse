#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-fasttest: needs Keeper and failpoints
# no-parallel: the failpoints are global, so a copy of this test running at the same time could pause on them

# Two attempts of one DDL query with the same `idempotency_key` on a Replicated database: the statement runs exactly
# once, whatever moment the first attempt is held at (before it is enqueued, before it commits, after it commits but
# before it answers, and while the other attempt is being validated), and a table replaced meanwhile by one with hidden
# tables is refused, and an attempt whose entry the database cancelled because it did not commit in time fails. An error
# of the statement itself is not hidden by the key. An attempt answered from the key waits for the synchronous
# completion that the first attempt asked for.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

R1="${CLICKHOUSE_DATABASE}_r1"
C1="${CLICKHOUSE_DATABASE}_c1"
ZK="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
FAILPOINTS="database_replicated_idempotency_key_pause_after_first_probe database_replicated_idempotency_key_pause_before_enqueue database_replicated_idempotency_key_pause_before_commit database_replicated_idempotency_key_pause_after_commit"

# q <query> [<name>=<value> ...]: runs the query over HTTP with these settings or HTTP parameters. A client process per
# query would make the test too slow under sanitizers.
function q()
{
    local query=$1 params=""
    shift
    for param in "$@"; do
        params+="&$param"
    done
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}${params}" --data-binary "$query"
}

function cleanup()
{
    for fp in $FAILPOINTS; do
        q "SYSTEM DISABLE FAILPOINT $fp" > /dev/null 2>&1
    done
    wait
    for db in $R1 $C1; do
        q "DROP DATABASE IF EXISTS $db SYNC"
    done
}
trap cleanup EXIT

# run <query> [<name>=<value> ...]: prints OK, or the name of the error.
function run()
{
    local out
    if out=$(q "$@" 2>&1); then
        echo OK
    else
        echo "$out" | grep -oE '\([A-Z][A-Z0-9_]+\)' | tail -1 | tr -d '()'
    fi
}

# keyed <key> <query> [<name>=<value> ...]
function keyed()
{
    local key=$1 query=$2
    shift 2
    run "$query" enable_ddl_idempotency_key=1 "idempotency_key=$key" distributed_ddl_output_mode=none "$@"
}

function plain()
{
    run "$1" distributed_ddl_output_mode=none
}

function comment_of()
{
    q "SELECT comment FROM system.tables WHERE database = '$1' AND name = '$2'"
}

function log_size()
{
    q "SELECT count() FROM system.zookeeper WHERE path = '$1/log'"
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

# wait_for <query>: waits until the query returns 1.
function wait_for()
{
    local deadline=$((SECONDS + 60))
    while [ "$(q "$1")" != 1 ] && [ $SECONDS -lt $deadline ]; do
        sleep 0.1
    done
}

function enable_failpoint()
{
    q "SYSTEM ENABLE FAILPOINT $1"
}

function release_failpoint()
{
    q "SYSTEM NOTIFY FAILPOINT $1"
    q "SYSTEM DISABLE FAILPOINT $1"
}

q "CREATE DATABASE $R1 ENGINE = Replicated('$ZK/r', 's1', 'r1')"
q "CREATE DATABASE $C1 ENGINE = Replicated('$ZK/c', 's1', 'r1') SETTINGS wait_entry_commited_timeout_sec = 1"
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
keyed k1 "EXCHANGE TABLES a AND b" "database=$R1" > "$CLICKHOUSE_TMP/a.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k1 "EXCHANGE TABLES a AND b" "database=$R1"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
comment_of "$R1" a

echo "-- the second attempt waits in the log for the first one, which is held before it commits"
fp=database_replicated_idempotency_key_pause_before_commit
enable_failpoint $fp
keyed k2 "EXCHANGE TABLES a AND b" "database=$R1" > "$CLICKHOUSE_TMP/a.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
size=$(log_size "$ZK/r")
keyed k2 "EXCHANGE TABLES a AND b" "database=$R1" > "$CLICKHOUSE_TMP/b.txt" &
wait_log_size "$ZK/r" "$size"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt" "$CLICKHOUSE_TMP/b.txt"
comment_of "$R1" a

echo "-- the database cancels the entry of the first attempt, which is held before it commits"
enable_failpoint $fp
keyed k3 "EXCHANGE TABLES a AND b" "database=$C1" > "$CLICKHOUSE_TMP/a.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k3 "EXCHANGE TABLES a AND b" "database=$C1"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
comment_of "$C1" a

echo "-- the first attempt committed, but its answer is held"
fp=database_replicated_idempotency_key_pause_after_commit
enable_failpoint $fp
keyed k4 "EXCHANGE TABLES a AND b" "database=$R1" > "$CLICKHOUSE_TMP/a.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k4 "EXCHANGE TABLES a AND b" "database=$R1"
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
    keyed "k5_$i" "$query" "database=$R1" > "$CLICKHOUSE_TMP/b.txt" &
    q "SYSTEM WAIT FAILPOINT $fp PAUSE"
    keyed "k5_$i" "$query" "database=$R1"
    [ "$i" = 3 ] && plain "DROP TABLE $R1.w2"
    release_failpoint $fp
    wait
    cat "$CLICKHOUSE_TMP/b.txt"
done
q "SELECT name FROM system.tables WHERE database = '$R1' ORDER BY name"

echo "-- errors of the statement itself"
keyed k6 "DROP TABLE $R1.never_existed"
enable_failpoint $fp
keyed k7 "DROP TABLE $R1.never_existed" > "$CLICKHOUSE_TMP/b.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
keyed k7 "CREATE TABLE $R1.q (x UInt8) ENGINE = Memory"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/b.txt"

echo "-- the table is replaced by one with hidden tables while the attempt is held before it is enqueued"
plain "CREATE TABLE $R1.h (x UInt8) ENGINE = Memory" > /dev/null
fp=database_replicated_idempotency_key_pause_before_enqueue
enable_failpoint $fp
keyed k8 "DROP TABLE $R1.h" > "$CLICKHOUSE_TMP/a.txt" &
q "SYSTEM WAIT FAILPOINT $fp PAUSE"
plain "DROP TABLE $R1.h"
plain "CREATE MATERIALIZED VIEW $R1.h ENGINE = MergeTree ORDER BY x AS SELECT x FROM $R1.a"
release_failpoint $fp
wait
cat "$CLICKHOUSE_TMP/a.txt"
q "SELECT engine FROM system.tables WHERE database = '$R1' AND name = 'h'"
plain "ALTER TABLE $R1.a MODIFY COMMENT 'after'"

echo "-- the first attempt asked for synchronous completion and waits for a query that still reads the dropped table"
plain "CREATE TABLE $R1.s (x UInt64) ENGINE = MergeTree ORDER BY x" > /dev/null
reader="${CLICKHOUSE_DATABASE}_reader"
q "SELECT count() FROM $R1.s WHERE x IN (SELECT sleepEachRow(3) FROM numbers(1000) SETTINGS max_block_size = 1)" "query_id=$reader" > /dev/null 2>&1 &
wait_for "SELECT count() > 0 FROM system.processes WHERE query_id = '$reader' AND read_rows > 0"
keys="SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/idempotency'"
keys_before=$(q "$keys")
keyed k9 "DROP TABLE $R1.s SYNC" database_replicated_enforce_synchronous_settings=1 > "$CLICKHOUSE_TMP/a.txt" &
wait_for "SELECT ($keys) > $keys_before"
keyed k9 "DROP TABLE $R1.s SYNC" database_replicated_enforce_synchronous_settings=1 distributed_ddl_task_timeout=1
q "KILL QUERY WHERE query_id = '$reader' SYNC FORMAT Null"
wait
cat "$CLICKHOUSE_TMP/a.txt"
keyed k9 "DROP TABLE $R1.s SYNC" database_replicated_enforce_synchronous_settings=1
