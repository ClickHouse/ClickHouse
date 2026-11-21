#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database
# no-parallel: every RENAME TABLE passes the server-wide failpoint the test waits on.

# Creating a database with the Ordinary engine logs a deprecation warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=rename_table_pause_after_locks
A="${CLICKHOUSE_DATABASE}_a"
B="${CLICKHOUSE_DATABASE}_b"
SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
SETTINGS="--allow_deprecated_database_ordinary=1 --allow_experimental_time_series_table=1"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    for db in $A $B $SRC $DST; do $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db" 2>/dev/null; done
}
trap cleanup EXIT

# Pause a rename while it holds the locks of database $1, start `DROP DATABASE $1` and wait until the drop waits for them.
function pause_rename_and_start_drop()
{
    local db=$1 rename_query=$2
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
    $CLICKHOUSE_CLIENT $SETTINGS -q "$rename_query" >/dev/null 2>&1 && echo "rename ok" > "$CLICKHOUSE_TMP/rename_result_$db" || echo "rename failed" > "$CLICKHOUSE_TMP/rename_result_$db" &
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $db SYNC" &
    for _ in {1..100}
    do
        $CLICKHOUSE_CLIENT -q "RENAME TABLE IF EXISTS $db.probe TO $db.probe2" 2>&1 | grep -q UNFINISHED && return
        sleep 0.1
    done
    echo "DROP DATABASE $db is not waiting for the rename"
}

# 1. While a drop of `A` is pending, no table of `A` can be renamed or exchanged, in or out; `B` is not affected.
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $A ENGINE = Atomic;
    CREATE DATABASE $B ENGINE = Atomic;
    CREATE TABLE $A.blocker (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE $A.t (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE $B.u (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO $B.u VALUES (1);
"
pause_rename_and_start_drop $A "RENAME TABLE $A.blocker TO $A.blocker2"
$CLICKHOUSE_CLIENT -q "RENAME TABLE $A.t TO $B.t" 2>&1 | grep -o -m1 UNFINISHED
$CLICKHOUSE_CLIENT -q "RENAME TABLE $B.u TO $A.u" 2>&1 | grep -o -m1 UNFINISHED
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES $A.t AND $B.u" 2>&1 | grep -o -m1 UNFINISHED
$CLICKHOUSE_CLIENT -q "RENAME TABLE $A.t TO $A.t2" 2>&1 | grep -o -m1 UNFINISHED
$CLICKHOUSE_CLIENT -q "RENAME TABLE $B.u TO $B.u2, $B.u2 TO $B.u"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait
cat "$CLICKHOUSE_TMP/rename_result_$A"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$A'"
$CLICKHOUSE_CLIENT -q "INSERT INTO $B.u VALUES (2); OPTIMIZE TABLE $B.u FINAL; SELECT count() FROM $B.u"

# 2. A rename that already holds the locks finishes before the drop, including the renames of inner tables
#    that `TimeSeries` and materialized views run in non-Atomic databases.
$CLICKHOUSE_CLIENT $SETTINGS -q "
    CREATE DATABASE $SRC ENGINE = Ordinary;
    CREATE DATABASE $DST ENGINE = Ordinary;
    CREATE TABLE $SRC.ts ENGINE = TimeSeries;
    CREATE TABLE $SRC.data (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW $SRC.mv ENGINE = MergeTree ORDER BY n AS SELECT n FROM $SRC.data;
"
pause_rename_and_start_drop $DST "RENAME TABLE $SRC.ts TO $DST.ts, $SRC.mv TO $DST.mv"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait
cat "$CLICKHOUSE_TMP/rename_result_$DST"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '$SRC' ORDER BY name"
