#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: needs Keeper

# A DDL query with `idempotency_key` on a Replicated database runs at most once for the key, and a retry succeeds like
# the original, also when it names the database differently. A key reused for another statement or by another user is
# an error, and it is not disclosed to a user who may not run the statement. A failed attempt records nothing, keys
# expire, and statements the key cannot be honoured for are refused.
# Where it matters, the statement is first repeated without a key, to show it is not idempotent on its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

R1="${CLICKHOUSE_DATABASE}_r1"
Y1="${CLICKHOUSE_DATABASE}_y1"
Z1="${CLICKHOUSE_DATABASE}_z1"
ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
ZK="/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
OTHER_USER="u_${CLICKHOUSE_DATABASE}"
DROP_USER="u_drop_${CLICKHOUSE_DATABASE}"

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
    for db in $R1 $Y1 $Z1 $ATOMIC; do
        q "DROP DATABASE IF EXISTS $db SYNC"
    done
    q "DROP USER IF EXISTS $OTHER_USER, $DROP_USER"
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

# refused <query> [<name>=<value> ...]: prints "refused" for the error a client matches on to run the statement without
# the key, else what keyed() prints
function refused()
{
    local out
    out=$(keyed k20 "$@")
    [ "$out" = IDEMPOTENCY_KEY_NOT_SUPPORTED ] && out=refused
    echo "$out"
}

function log_size()
{
    q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/log'"
}

function plain()
{
    run "$1" distributed_ddl_output_mode=none
}

function comment_of()
{
    q "SELECT comment FROM system.tables WHERE database = '$1' AND name = '$2'"
}

function has_column()
{
    q "SELECT count() FROM system.columns WHERE database = '$1' AND table = '$2' AND name = '$3'"
}

function tables_named()
{
    q "SELECT count() FROM system.tables WHERE database = '$1' AND name = '$2'"
}

q "CREATE DATABASE $R1 ENGINE = Replicated('$ZK/r', 's1', 'r1')"
for t in a b c d; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory COMMENT '$t'" > /dev/null
done
for t in x z z0 w dst; do
    plain "CREATE TABLE $R1.$t (x UInt8) ENGINE = Memory" > /dev/null
done
plain "CREATE TABLE $R1.t (x UInt8) ENGINE = MergeTree ORDER BY x" > /dev/null
plain "CREATE TABLE $R1.rmt (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x" > /dev/null

echo "-- the key needs enable_ddl_idempotency_key"
run "CREATE TABLE $R1.g (x UInt8) ENGINE = Memory" idempotency_key=k0

echo "-- EXCHANGE twice without a key swaps back, with a key it swaps once"
plain "EXCHANGE TABLES $R1.c AND $R1.d"
plain "EXCHANGE TABLES $R1.c AND $R1.d"
comment_of "$R1" c
keyed k1 "EXCHANGE TABLES $R1.a AND $R1.b"
keyed k1 "EXCHANGE TABLES a AND b" "database=$R1"
comment_of "$R1" a

echo "-- ALTER ADD COLUMN"
plain "ALTER TABLE $R1.t ADD COLUMN c0 UInt8"
plain "ALTER TABLE $R1.t ADD COLUMN c0 UInt8"
keyed k2 "ALTER TABLE $R1.rmt ADD COLUMN c UInt8"
keyed k2 "ALTER TABLE rmt ADD COLUMN c UInt8" "database=$R1"
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
keyed k5 "DROP TABLE $R1.z" ignore_drop_queries_probability=1
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
keyed k17 "CREATE DICTIONARY $R1.kd (k UInt64, v UInt8) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
size=$(log_size)
keyed k17 "CREATE DICTIONARY $R1.kd (k UInt64, v UInt8) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
echo "the retry made $(( $(log_size) - size )) log entries"
keyed k18 "CREATE TABLE $R1.nm (x UInt8) ENGINE = Memory"
size=$(log_size)
keyed k18 "CREATE TABLE $R1.nm (x UInt8) ENGINE = Memory"
echo "the retry made $(( $(log_size) - size )) log entries"
tables_named "$R1" n
tables_named "$R1" v
tables_named "$R1" mv
q "SELECT count() FROM system.dictionaries WHERE database = '$R1' AND name = 'kd'"
tables_named "$R1" nm

echo "-- ALTER of a view, a materialized view with TO and a Buffer table"
plain "CREATE TABLE $R1.buf (x UInt8) ENGINE = Buffer($R1, dst, 1, 10, 100, 10000, 1000000, 10000000, 100000000)" > /dev/null
for t in v mv buf; do
    keyed "k26$t" "ALTER TABLE $R1.$t MODIFY COMMENT 'altered'"
    size=$(log_size)
    keyed "k26$t" "ALTER TABLE $R1.$t MODIFY COMMENT 'altered'"
    echo "the retry made $(( $(log_size) - size )) log entries"
    comment_of "$R1" "$t"
done

echo "-- a key used by another statement or user"
keyed k1 "EXCHANGE TABLES $R1.c AND $R1.d"
q "CREATE USER $OTHER_USER IDENTIFIED WITH no_password"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8" "user=$OTHER_USER"
q "GRANT ALTER ADD COLUMN ON $R1.t TO $OTHER_USER"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8" "user=$OTHER_USER"
echo "-- the output clauses of the retry do not matter"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8 SETTINGS distributed_ddl_output_mode = 'throw'"
echo "-- nor does a retry wait for replicas to sync if the original did not ask for it"
keyed k3 "ALTER TABLE $R1.t ADD COLUMN c UInt8" database_replicated_enforce_synchronous_settings=1 distributed_ddl_task_timeout=10

echo "-- an answer from the key requires the access the statement requires"
q "GRANT ALTER ADD COLUMN ON $R1.t TO $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" "user=$OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" "user=$OTHER_USER"
q "REVOKE ALTER ADD COLUMN ON $R1.t FROM $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" "user=$OTHER_USER"
plain "CREATE VIEW $R1.dt AS SELECT 1" > /dev/null
q "CREATE USER $DROP_USER IDENTIFIED WITH no_password"
q "GRANT DROP VIEW ON $R1.dt TO $DROP_USER"
keyed k14 "DROP TABLE $R1.dt" "user=$DROP_USER"
keyed k14 "DROP TABLE $R1.dt" "user=$DROP_USER"
plain "CREATE DICTIONARY $R1.dd (k UInt64, v UInt8) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)" > /dev/null
plain "CREATE TABLE $R1.de (k UInt64, v UInt8) ENGINE = Dictionary($R1.dd)" > /dev/null
q "GRANT DROP DICTIONARY ON $R1.de TO $DROP_USER"
keyed k16 "DROP TABLE $R1.de" "user=$DROP_USER"
keyed k16 "DROP TABLE $R1.de" "user=$DROP_USER"
echo "-- a recreated user with the same name is another user"
q "DROP USER $OTHER_USER"
q "CREATE USER $OTHER_USER IDENTIFIED WITH no_password"
q "GRANT ALTER ADD COLUMN ON $R1.t TO $OTHER_USER"
keyed k10 "ALTER TABLE $R1.t ADD COLUMN u UInt8" "user=$OTHER_USER"

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
refused "INSERT INTO $R1.t (x) VALUES (1)" async_insert=0
refused "INSERT INTO $R1.t (x) VALUES (1)" async_insert=1
q "CREATE DATABASE $ATOMIC ENGINE = Atomic"
refused "CREATE TABLE $ATOMIC.r (x UInt8) ENGINE = Memory"
refused "ALTER TABLE $R1.km ADD COLUMN x UInt8"
plain "CREATE TABLE $ATOMIC.at (x UInt8) ENGINE = MergeTree ORDER BY x" > /dev/null
plain "CREATE TABLE $R1.al ENGINE = Alias('$ATOMIC', 'at')" > /dev/null
refused "ALTER TABLE $R1.al ADD COLUMN y UInt8"
keyed k20 "SELECT 1"
tables_named "$R1" r
tables_named "$R1" a
has_column "$ATOMIC" at y

echo "-- a user without the access is refused before the database or the key is looked at"
version=$(q "SELECT version FROM system.zookeeper WHERE path = '$ZK/r' AND name = 'idempotency'")
keyed k27 "CREATE TABLE $ATOMIC.na (x UInt8) ENGINE = Memory" "user=$OTHER_USER"
keyed k27 "CREATE TABLE $R1.na (x UInt8) ENGINE = Memory" "user=$OTHER_USER"
[ "$(q "SELECT version FROM system.zookeeper WHERE path = '$ZK/r' AND name = 'idempotency'")" = "$version" ] && echo "no Keeper write"

echo "-- a key in the settings of a statement nested in another query is refused"
q "GRANT CREATE TABLE ON $R1.ea TO $OTHER_USER"
q "GRANT TABLE ENGINE ON MergeTree TO $OTHER_USER"
plain "ALTER TABLE $R1.t ADD COLUMN p UInt8 SETTINGS idempotency_key = 'k21', enable_ddl_idempotency_key = 1 PARALLEL WITH ALTER TABLE $R1.dst MODIFY COMMENT 'p'"
plain "EXECUTE AS $OTHER_USER CREATE TABLE $R1.ea (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS idempotency_key = 'k22', enable_ddl_idempotency_key = 1"
has_column "$R1" t p
tables_named "$R1" ea

echo "-- the key travels neither with the settings of the log entry nor with its text"
plain "ALTER TABLE $R1.t ADD COLUMN c5 UInt8 SETTINGS idempotency_key = 'k12', enable_ddl_idempotency_key = 1"
plain "ALTER TABLE $R1.t ADD COLUMN c5 UInt8 SETTINGS idempotency_key = 'k12', enable_ddl_idempotency_key = 1"
has_column "$R1" t c5
plain "ALTER TABLE $R1.t ADD COLUMN c6 UInt8 SETTINGS idempotency_key = 'k19', idempotency_key = 'k19', enable_ddl_idempotency_key = 1"
plain "ALTER TABLE $R1.t ADD COLUMN c7 UInt8 SETTINGS idempotency_key = 'k24', enable_ddl_idempotency_key = 1, max_threads = DEFAULT"
plain "ALTER TABLE $R1.t MODIFY COMMENT 'no key' SETTINGS enable_ddl_idempotency_key = 1"
q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/log' AND (value LIKE '%idempotency_key = %' OR value LIKE '%enable_ddl_idempotency_key%')"
q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/r/log' AND value LIKE '%ADD COLUMN \`c7\` UInt8) SETTINGS max_threads = DEFAULT%'"

echo "-- the key is not sent to the shards of a distributed query"
qid="${CLICKHOUSE_DATABASE}_remote_${RANDOM}"
q "SELECT count() FROM remote('127.0.0.2', system.one)" enable_ddl_idempotency_key=1 idempotency_key=k23 prefer_localhost_replica=0 log_queries=1 "query_id=$qid"
q "SYSTEM FLUSH LOGS query_log"
q "
    SELECT count() > 0, countIf(Settings['idempotency_key'] != '' OR Settings['enable_ddl_idempotency_key'] != '')
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
        AND initial_query_id = (SELECT any(query_id) FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$qid')"

echo "-- keys expire"
q "CREATE DATABASE $Y1 ENGINE = Replicated('$ZK/y', 's1', 'r1') SETTINGS idempotency_key_ttl_sec = 10"
plain "CREATE TABLE $Y1.e (x UInt8) ENGINE = Memory" > /dev/null
keyed k13 "ALTER TABLE $Y1.e ADD COLUMN c UInt8"
plain "ALTER TABLE $Y1.e DROP COLUMN c"
deadline=$((SECONDS + 45))
while [ $SECONDS -lt $deadline ]; do
    keyed k13 "ALTER TABLE $Y1.e ADD COLUMN c UInt8" > /dev/null
    [ "$(has_column "$Y1" e c)" = 1 ] && break
    sleep 0.5
done
keyed k13 "ALTER TABLE $Y1.e ADD COLUMN c UInt8"
has_column "$Y1" e c

echo "-- an expired key is pruned without a later key commit"
q "CREATE DATABASE $Z1 ENGINE = Replicated('$ZK/z', 's1', 'r1') SETTINGS idempotency_key_ttl_sec = 1"
plain "CREATE TABLE $Z1.e (x UInt8) ENGINE = Memory" > /dev/null
keyed k25 "ALTER TABLE $Z1.e ADD COLUMN c UInt8"
# A replica prunes when its database starts, then hourly.
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    q "DETACH DATABASE $Z1"
    q "ATTACH DATABASE $Z1"
    [ "$(q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/z/idempotency'")" = 0 ] && break
    sleep 1
done
q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/z/idempotency'"
