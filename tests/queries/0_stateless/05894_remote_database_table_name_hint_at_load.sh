#!/usr/bin/env bash

# The hint for an unknown table lists the table names of every database. A `Remote` database whose shard is local must
# not wait there for the tables of its local database to load: during the load of a table whose projection reads a
# dropped table, one of them is the table being loaded. The projection is skipped at load, so the error is checked in
# `system.errors`. The hint must still list the names, including the table being loaded, and only the visible ones.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}
src="${CLICKHOUSE_DATABASE}_src"
rdb="${CLICKHOUSE_DATABASE}_remote"
user="u_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${rdb}; DROP DATABASE IF EXISTS ${src}; DROP USER IF EXISTS ${user}"
    rm -rf "$dir"
}
trap cleanup EXIT

rm -rf "$dir"
mkdir -p "$dir/extended_hint" "$dir/direct_hint"

errors="SELECT name FROM system.errors WHERE name IN ('UNKNOWN_TABLE', 'LOGICAL_ERROR') ORDER BY name"

echo "--- a Remote database over the database of the table"
$CLICKHOUSE_LOCAL --path "$dir/extended_hint" -q "
    CREATE DATABASE d1;
    CREATE TABLE d1.source_table (b UInt32) ENGINE = MergeTree ORDER BY b;
    CREATE TABLE d1.table_with_projection (a UInt32, b UInt32, PROJECTION p (SELECT a, b WHERE b IN (SELECT b FROM d1.source_table) ORDER BY b)) ENGINE = MergeTree ORDER BY a;
    DROP TABLE d1.source_table SYNC;
    CREATE DATABASE r ENGINE = Remote('127.0.0.1', 'd1');
"
$CLICKHOUSE_LOCAL --path "$dir/extended_hint" -q "SELECT count() FROM d1.table_with_projection; $errors"

echo "--- the hint through a Remote database lists the table that is loading"
$CLICKHOUSE_LOCAL --path "$dir/direct_hint" -q "
    CREATE DATABASE d1;
    CREATE DATABASE r ENGINE = Remote('127.0.0.1', 'd1');
    CREATE TABLE d1.table_with_projectio (b UInt32) ENGINE = MergeTree ORDER BY b;
    CREATE TABLE d1.table_with_projection (a UInt32, b UInt32, PROJECTION p (SELECT a, b WHERE b IN (SELECT b FROM r.table_with_projectio) ORDER BY b)) ENGINE = MergeTree ORDER BY a;
    DROP TABLE d1.table_with_projectio SYNC;
"
$CLICKHOUSE_LOCAL --path "$dir/direct_hint" -q "
    SELECT count() FROM d1.table_with_projection FORMAT Null;
    SELECT last_error_message FROM system.errors WHERE name = 'UNKNOWN_TABLE';
" | grep -o -m1 "Maybe you meant [^?]*"

echo "--- the hint of a Remote database lists only the local tables the user can see"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${src};
    CREATE TABLE ${src}.hidden_table (x UInt8) ENGINE = Memory;
    CREATE TABLE ${src}.visible_table (x UInt8) ENGINE = Memory;
    CREATE DATABASE ${rdb} ENGINE = Remote('127.0.0.1', '${src}');
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${rdb}.* TO ${user};
    GRANT SELECT ON ${src}.visible_table TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${rdb}.hidden_tabl" 2>&1 | grep -q "Maybe you meant .*hidden_table" && echo "hidden_table suggested" || echo "hidden_table not suggested"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${rdb}.visible_tabl" 2>&1 | grep -q "Maybe you meant ${rdb}.visible_table" && echo "visible_table suggested" || echo "visible_table not suggested"
