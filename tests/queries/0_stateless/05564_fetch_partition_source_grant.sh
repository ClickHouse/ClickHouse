#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: FETCH PARTITION is not supported by SharedMergeTree
# no-replicated-database: the test creates replicas with explicit Keeper paths and replica names

# `FETCH PARTITION|PART ... FROM '<path>'` copies the data of the tables that use `<path>`, so it requires
# `SELECT` on every table of this server that uses the path (`SELECT ON *.*` when none does), and `INSERT`
# on the destination in addition to `ALTER FETCH PARTITION`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX"
user="${CLICKHOUSE_DATABASE}_u_05564"
user_noins="${CLICKHOUSE_DATABASE}_noins_05564"
user_glob="${CLICKHOUSE_DATABASE}_glob_05564"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS src_r2 SYNC;
DROP TABLE IF EXISTS dst SYNC;
DROP TABLE IF EXISTS mv SYNC;
DROP TABLE IF EXISTS mv_src;
DROP USER IF EXISTS $user, $user_noins, $user_glob;

CREATE TABLE src (id UInt32, secret String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') PARTITION BY id ORDER BY id;
INSERT INTO src VALUES (1, 'a'), (2, 'b');
CREATE TABLE dst (id UInt32, secret String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1') PARTITION BY id ORDER BY id;
CREATE TABLE mv_src (id UInt32, secret String) ENGINE = Null;
CREATE MATERIALIZED VIEW mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv', 'r1') PARTITION BY id ORDER BY id AS SELECT * FROM mv_src;

CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, INSERT, ALTER FETCH PARTITION ON $CLICKHOUSE_DATABASE.dst TO $user;
GRANT SELECT, INSERT, ALTER FETCH PARTITION ON $CLICKHOUSE_DATABASE.mv TO $user;

CREATE USER $user_noins IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON $CLICKHOUSE_DATABASE.src TO $user_noins;
GRANT ALTER FETCH PARTITION ON $CLICKHOUSE_DATABASE.dst TO $user_noins;

CREATE USER $user_glob IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON *.* TO $user_glob;
GRANT INSERT, ALTER FETCH PARTITION ON $CLICKHOUSE_DATABASE.dst TO $user_glob;
"

part=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND partition_id = '2' AND active")

# Prints `OK`, or the grant an `ACCESS_DENIED` asks for, or the output of any other error.
function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$1" --password "password" -q "$2" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        local reason
        if echo "$output" | grep -qF "that use this path, or SELECT ON *.* if no table of this server uses it"; then
            reason="SELECT on the tables that use the path, or SELECT ON *.*"
        elif echo "$output" | grep -q "Missing permissions: "; then
            reason=$(echo "$output" | grep -m1 -o "Missing permissions: .*). (ACCESS_DENIED)" | sed -e 's/^Missing permissions: //' -e 's/). (ACCESS_DENIED)$//')
        else
            reason=$(echo "$output" | grep -m1 -o "necessary to have the grant .*. (ACCESS_DENIED)" | sed -e 's/^necessary to have the grant //' -e 's/. (ACCESS_DENIED)$//')
        fi
        reason="${reason//${CLICKHOUSE_DATABASE_1}/db1}"
        echo "ACCESS_DENIED: ${reason//${CLICKHOUSE_DATABASE}/db}"
    else
        echo "$output"
    fi
}

echo "-- a table the user cannot see is not named"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/elsewhere'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/elsewhere'" 2>&1 | grep -c "ACCESS_DENIED"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE.src TO $user"

echo "-- the source is not readable"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"
check_access "$user" "ALTER TABLE dst FETCH PART '$part' FROM '$zk/src'"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src//'"

echo "-- a materialized view passes the command to its target, which checks the source"
check_access "$user" "ALTER TABLE mv FETCH PARTITION 1 FROM '$zk/src'"

echo "-- a grant on some columns is not a grant to read the table"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $CLICKHOUSE_DATABASE.src TO $user"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"

echo "-- the destination needs INSERT"
check_access "$user_noins" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"

echo "-- every table of this server that uses the path must be readable"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE src_r2 (id UInt32, secret String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r2') PARTITION BY id ORDER BY id;
SYSTEM SYNC REPLICA src_r2;
GRANT SELECT ON $CLICKHOUSE_DATABASE.src TO $user;
GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE.src_r2 TO $user;
"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"

echo "-- allowed once all of them are readable"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.src_r2 TO $user"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/src'"
check_access "$user" "ALTER TABLE dst FETCH PART '$part' FROM '$zk/src'"
check_access "$user" "ALTER TABLE dst ATTACH PARTITION 1"
check_access "$user" "ALTER TABLE dst ATTACH PART '$part'"
$CLICKHOUSE_CLIENT --user "$user" --password "password" -q "SELECT * FROM dst ORDER BY id"

echo "-- the same path on an auxiliary ZooKeeper is another source"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM 'zookeeper2:$zk/src'"

echo "-- SELECT ON *.* allows a source no table of this server uses"
$CLICKHOUSE_CLIENT --user "$user_glob" --password "password" -q "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/elsewhere'" 2>&1 | grep -o -m1 "KEEPER_EXCEPTION\|ACCESS_DENIED"

echo "-- a lazily loaded source is checked without loading it"
$CLICKHOUSE_CLIENT -q "
DROP DATABASE IF EXISTS $CLICKHOUSE_DATABASE_1;
CREATE DATABASE $CLICKHOUSE_DATABASE_1 ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $CLICKHOUSE_DATABASE_1.lsrc (id UInt32, secret String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/lsrc', 'r1') PARTITION BY id ORDER BY id;
INSERT INTO $CLICKHOUSE_DATABASE_1.lsrc VALUES (1, 'a');
DETACH DATABASE $CLICKHOUSE_DATABASE_1;
ATTACH DATABASE $CLICKHOUSE_DATABASE_1;
SELECT engine FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE_1' AND name = 'lsrc';
"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON $CLICKHOUSE_DATABASE_1.lsrc TO $user"
check_access "$user" "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/lsrc'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE_1.lsrc TO $user"
$CLICKHOUSE_CLIENT --user "$user" --password "password" -q "ALTER TABLE dst FETCH PARTITION 1 FROM '$zk/lsrc'" 2>&1 | grep -o -m1 "NO_ACTIVE_REPLICAS\|ACCESS_DENIED"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE_1' AND name = 'lsrc'"

$CLICKHOUSE_CLIENT -q "
DROP DATABASE $CLICKHOUSE_DATABASE_1 SYNC;
DROP TABLE src SYNC;
DROP TABLE src_r2 SYNC;
DROP TABLE dst SYNC;
DROP TABLE mv SYNC;
DROP TABLE mv_src;
DROP USER $user, $user_noins, $user_glob;
"
