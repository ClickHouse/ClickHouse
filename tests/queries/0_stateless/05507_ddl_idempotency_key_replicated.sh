#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: needs Keeper

# A DDL query with `idempotency_key` on a Replicated database runs at most once for the key, and a retry succeeds like
# the original, also when it names the database differently. A key reused for another statement or by another user is
# an error, a failed attempt records nothing, keys expire, and statements the key cannot be honoured for are refused.
# Where it matters, the statement is first repeated without a key, to show it is not idempotent on its own.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

R1="${CLICKHOUSE_DATABASE}_r1"
Y1="${CLICKHOUSE_DATABASE}_y1"
ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
ZK="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
OTHER_USER="u_${CLICKHOUSE_DATABASE}"
DROP_USER="u_drop_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    for db in $R1 $Y1 $ATOMIC; do
        $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
    done
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $OTHER_USER, $DROP_USER"
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

# keyed <key> <query> [client options]
function keyed()
{
    local key=$1 query=$2
    shift 2
    run $CLICKHOUSE_CLIENT --enable_ddl_idempotency_key 1 --idempotency_key "$key" --distributed_ddl_output_mode none "$@" -q "$query"
}

# refused <query> [client options]: prints "refused" if the key refused the statement, else what keyed() prints
function refused()
{
    local query=$1 out
    shift
    if out=$($CLICKHOUSE_CLIENT --enable_ddl_idempotency_key 1 --idempotency_key k20 --distributed_ddl_output_mode none "$@" -q "$query" 2>&1); then
        echo OK
    elif echo "$out" | grep -qE 'idempotency_key is (not supported for|supported only for statements)'; then
        echo refused
    else
        echo "$out" | grep -oE '\([A-Z][A-Z0-9_]+\)' | tail -1 | tr -d '()'
    fi
}

function log_size()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/log'"
}

function plain()
{
    run $CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "$1"
}

function comment_of()
{
    $CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = '$1' AND name = '$2'"
}

function has_column()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = '$1' AND table = '$2' AND name = '$3'"
}

function tables_named()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '$1' AND name = '$2'"
}

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $R1 ENGINE = Replicated('$ZK/r', 's1', 'r1')"
for t in a b c d; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory COMMENT '$t'" > /dev/null
done
for t in x z z0 w dst; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory" > /dev/null
done
plain "CREATE TABLE $R1.t (x UInt8) ENGINE = MergeTree ORDER BY x" > /dev/null
plain "CREATE TABLE $R1.rmt (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x" > /dev/null

echo "-- the key needs enable_ddl_idempotency_key"
run $CLICKHOUSE_CLIENT --idempotency_key k0 -q "CREATE TABLE $R1.g (x UInt8) ENGINE = Memory"

echo "-- EXCHANGE twice without a key swaps back, with a key it swaps once"
plain "EXCHANGE TABLES $R1.c AND $R1.d"
plain "EXCHANGE TABLES $R1.c AND $R1.d"
comment_of "$R1" c
keyed k1 "EXCHANGE TABLES $R1.a AND $R1.b"
keyed k1 "USE $R1; EXCHANGE TABLES a AND b"
comment_of "$R1" a

echo "-- ALTER ADD COLUMN"
plain "ALTER TABLE $R1.t ADD COLUMN c0 UInt8"
plain "ALTER TABLE $R1.t ADD COLUMN c0 UInt8"
keyed k2 "ALTER TABLE $R1.rmt ADD COLUMN c UInt8"
keyed k2 "USE $R1; ALTER TABLE rmt ADD COLUMN c UInt8"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8"
size=$(log_size)
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8"
echo "the retry made $(( $(log_size) - size )) log entries"
has_column "$R1" rmt c
has_column "$R1" t c
echo "-- the IF EXISTS of an ALTER command is not refused"
keyed k15 "ALTER TABLE $R1.t DROP COLUMN IF EXISTS missing"
size=$(log_size)
keyed k15 "ALTER TABLE $R1.t DROP COLUMN IF EXISTS missing"
echo "the retry made $(( $(log_size) - size )) log entries"

echo "-- RENAME and DROP"
plain "RENAME TABLE $R1.x TO $R1.x0"
plain "RENAME TABLE $R1.x TO $R1.x0"
keyed k4 "RENAME TABLE $R1.x0 TO $R1.y"
keyed k4 "RENAME TABLE $R1.x0 TO $R1.y"
plain "DROP TABLE $R1.z0"
plain "DROP TABLE $R1.z0"
keyed k5 "DROP TABLE $R1.z" --ignore_drop_queries_probability 1
keyed k5 "DROP TABLE $R1.z"
tables_named "$R1" y
tables_named "$R1" z

echo "-- a retry is answered from the key although a later statement dropped the table"
keyed k6 "ALTER TABLE $R1.w ADD COLUMN c UInt8"
plain "DROP TABLE $R1.w"
keyed k6 "ALTER TABLE $R1.w ADD COLUMN c UInt8"

echo "-- CREATE"
plain "CREATE TABLE $R1.n0 (x UInt8) ENGINE = Memory"
plain "CREATE TABLE $R1.n0 (x UInt8) ENGINE = Memory"
keyed k7 "CREATE TABLE $R1.n (x UInt8) ENGINE = MergeTree ORDER BY x"
keyed k7 "CREATE TABLE $R1.n (x UInt8) ENGINE = MergeTree ORDER BY x"
keyed k8 "CREATE VIEW $R1.v AS SELECT 1"
keyed k8 "CREATE VIEW $R1.v AS SELECT 1"
keyed k9 "CREATE MATERIALIZED VIEW $R1.mv TO $R1.dst AS SELECT x FROM $R1.t"
keyed k9 "CREATE MATERIALIZED VIEW $R1.mv TO $R1.dst AS SELECT x FROM $R1.t"
tables_named "$R1" n
tables_named "$R1" v
tables_named "$R1" mv

echo "-- a key used by another statement or user"
keyed k1 "EXCHANGE TABLES $R1.c AND $R1.d"
$CLICKHOUSE_CLIENT -q "CREATE USER $OTHER_USER IDENTIFIED WITH no_password"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8" --user "$OTHER_USER"
echo "-- the output clauses of the retry do not matter"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8 SETTINGS distributed_ddl_output_mode = 'throw'"
echo "-- nor does a retry wait for replicas to sync if the original did not ask for it"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8" --database_replicated_enforce_synchronous_settings 1 --distributed_ddl_task_timeout 10

echo "-- an answer from the key requires the access the statement requires"
$CLICKHOUSE_CLIENT -q "GRANT ALTER ADD COLUMN ON $R1.t TO $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" --user "$OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" --user "$OTHER_USER"
$CLICKHOUSE_CLIENT -q "REVOKE ALTER ADD COLUMN ON $R1.t FROM $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" --user "$OTHER_USER"
plain "CREATE VIEW $R1.dt AS SELECT 1" > /dev/null
$CLICKHOUSE_CLIENT -q "CREATE USER $DROP_USER IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT DROP VIEW ON $R1.dt TO $DROP_USER"
keyed k14 "DROP TABLE $R1.dt" --user "$DROP_USER"
keyed k14 "DROP TABLE $R1.dt" --user "$DROP_USER"
plain "CREATE DICTIONARY $R1.dd (k UInt64, v UInt8) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)" > /dev/null
plain "CREATE TABLE $R1.de (k UInt64, v UInt8) ENGINE = Dictionary($R1.dd)" > /dev/null
$CLICKHOUSE_CLIENT -q "GRANT DROP DICTIONARY ON $R1.de TO $DROP_USER"
keyed k16 "DROP TABLE $R1.de" --user "$DROP_USER"
keyed k16 "DROP TABLE $R1.de" --user "$DROP_USER"
echo "-- a recreated user with the same name is another user"
$CLICKHOUSE_CLIENT -q "DROP USER $OTHER_USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $OTHER_USER IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT ALTER ADD COLUMN ON $R1.t TO $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" --user "$OTHER_USER"

echo "-- a failed attempt records nothing"
keyed k11 "ALTER TABLE $R1.t ADD COLUMN c UInt8"
plain "ALTER TABLE $R1.t DROP COLUMN c"
keyed k11 "ALTER TABLE $R1.t ADD COLUMN c UInt8"
has_column "$R1" t c

echo "-- refused statements"
plain "CREATE TABLE $R1.km (k UInt64, v UInt64) ENGINE = KeeperMap('$ZK/km') PRIMARY KEY k" > /dev/null
plain "CREATE MATERIALIZED VIEW $R1.mvi ENGINE = MergeTree ORDER BY x AS SELECT x FROM $R1.t" > /dev/null
refused "CREATE TABLE $R1.r (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x"
refused "CREATE TABLE $R1.r (x UInt8)"
refused "CREATE TABLE $R1.r (k UInt64, v UInt64) ENGINE = KeeperMap('$ZK/km2') PRIMARY KEY k"
refused "DROP TABLE $R1.a, $R1.b"
refused "RENAME TABLE $R1.a TO $R1.a1, $R1.b TO $R1.b1"
refused "DROP TABLE IF EXISTS $R1.missing"
refused "RENAME TABLE IF EXISTS $R1.missing TO $R1.m2"
refused "TRUNCATE TABLE $R1.t"
refused "DETACH TABLE $R1.t PERMANENTLY"
refused "ATTACH TABLE $R1.t"
refused "CREATE OR REPLACE TABLE $R1.t (x UInt8) ENGINE = Memory"
refused "ALTER TABLE $R1.t DELETE WHERE 1"
refused "ALTER TABLE $R1.t UPDATE x = 1 WHERE 1"
refused "DELETE FROM $R1.t WHERE 1"
refused "UPDATE $R1.t SET x = 1 WHERE 1"
refused "ALTER TABLE $R1.rmt DROP PARTITION ID 'all'"
refused "ALTER TABLE $R1.rmt CLEAR COLUMN c IN PARTITION ID 'all'"
refused "CREATE TABLE $R1.r ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x"
refused "CREATE MATERIALIZED VIEW $R1.r TO $R1.dst POPULATE AS SELECT x FROM $R1.t"
refused "CREATE MATERIALIZED VIEW $R1.r ENGINE = MergeTree ORDER BY x AS SELECT x FROM $R1.t"
refused "DROP TABLE $R1.mvi"
refused "ALTER TABLE $R1.mvi MODIFY COMMENT 'c'"
refused "DROP DATABASE $R1"
refused "CREATE TABLE $R1.r ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory"
refused "INSERT INTO $R1.t (x) VALUES (1)" --async_insert 0
refused "INSERT INTO $R1.t (x) VALUES (1)" --async_insert 1
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $ATOMIC ENGINE = Atomic"
refused "CREATE TABLE $ATOMIC.r (x UInt8) ENGINE = Memory"
refused "ALTER TABLE $R1.km ADD COLUMN x UInt8"
keyed k20 "SELECT 1"
tables_named "$R1" r
tables_named "$R1" a

echo "-- the key travels neither with the settings of the log entry nor with its text"
plain "ALTER TABLE $R1.t ADD COLUMN c5 UInt8 SETTINGS idempotency_key = 'k12', enable_ddl_idempotency_key = 1"
plain "ALTER TABLE $R1.t ADD COLUMN c5 UInt8 SETTINGS idempotency_key = 'k12', enable_ddl_idempotency_key = 1"
has_column "$R1" t c5
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/log' AND (value LIKE '%idempotency_key = %' OR value LIKE '%enable_ddl_idempotency_key%')"

echo "-- keys expire"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $Y1 ENGINE = Replicated('$ZK/y', 's1', 'r1') SETTINGS idempotency_key_ttl_sec = 1"
plain "CREATE TABLE $Y1.e (x UInt8) ENGINE = Memory" > /dev/null
keyed k13 "ALTER TABLE $Y1.e ADD COLUMN c UInt8"
plain "ALTER TABLE $Y1.e DROP COLUMN c"
deadline=$((SECONDS + 30))
while [ "$(has_column "$Y1" e c)" = 0 ] && [ $SECONDS -lt $deadline ]; do
    keyed k13 "ALTER TABLE $Y1.e ADD COLUMN c UInt8" > /dev/null
    sleep 0.5
done
has_column "$Y1" e c

