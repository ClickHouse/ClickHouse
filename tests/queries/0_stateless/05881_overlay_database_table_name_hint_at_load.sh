#!/usr/bin/env bash

# The hint for an unknown table lists the table names of every database. An `Overlay` database must not wait for the
# tables of its sources to load there: during the load of a table whose projection reads a dropped table, one of them is
# the table being loaded. The projection is skipped at load, so the error is checked in `system.errors`.
# The hint must still list the names, and only the visible ones.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}
src="${CLICKHOUSE_DATABASE}_src"
ov="${CLICKHOUSE_DATABASE}_ov"
user="u_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ov}; DROP DATABASE IF EXISTS ${src}; DROP USER IF EXISTS ${user}"
    rm -rf "$dir"
}
trap cleanup EXIT

rm -rf "$dir"
mkdir -p "$dir"

errors="SELECT name FROM system.errors WHERE name IN ('UNKNOWN_TABLE', 'LOGICAL_ERROR') ORDER BY name"

echo "--- the default database of clickhouse-local"
$CLICKHOUSE_LOCAL --path "$dir/1" -q "
    CREATE TABLE source_table (b UInt32) ENGINE = MergeTree ORDER BY b;
    CREATE TABLE table_with_projection (a UInt32, b UInt32, PROJECTION p (SELECT a, b WHERE b IN (SELECT b FROM source_table) ORDER BY b)) ENGINE = MergeTree ORDER BY a;
    DROP TABLE source_table SYNC;
"
$CLICKHOUSE_LOCAL --path "$dir/1" -q "SELECT count() FROM table_with_projection; $errors"
$CLICKHOUSE_LOCAL --path "$dir/1" -q "SELECT * FROM table_with_projectio" 2>&1 | grep -o -m1 "Maybe you meant [^?]*"

echo "--- an Overlay database over the database of the table"
$CLICKHOUSE_LOCAL --path "$dir/2" -q "
    CREATE DATABASE d1;
    CREATE TABLE d1.source_table (b UInt32) ENGINE = MergeTree ORDER BY b;
    CREATE TABLE d1.table_with_projection (a UInt32, b UInt32, PROJECTION p (SELECT a, b WHERE b IN (SELECT b FROM d1.source_table) ORDER BY b)) ENGINE = MergeTree ORDER BY a;
    DROP TABLE d1.source_table SYNC;
    CREATE DATABASE ov ENGINE = Overlay('d1');
"
$CLICKHOUSE_LOCAL --path "$dir/2" -q "SELECT count() FROM d1.table_with_projection; $errors"
$CLICKHOUSE_LOCAL --path "$dir/2" -q "SELECT * FROM ov.table_with_projectio" 2>&1 | grep -o -m1 "Maybe you meant [^?]*"

echo "--- the hint of an Overlay database lists only the source tables the user can see"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${src};
    CREATE TABLE ${src}.hidden_table (x UInt8) ENGINE = Memory;
    CREATE TABLE ${src}.visible_table (x UInt8) ENGINE = Memory;
    CREATE DATABASE ${ov} ENGINE = Overlay('${src}');
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${ov}.* TO ${user};
    GRANT SELECT ON ${src}.visible_table TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${ov}.hidden_tabl" 2>&1 | grep -q "Maybe you meant .*hidden_table" && echo "hidden_table suggested" || echo "hidden_table not suggested"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${ov}.visible_tabl" 2>&1 | grep -q "Maybe you meant ${ov}.visible_table" && echo "visible_table suggested" || echo "visible_table not suggested"
