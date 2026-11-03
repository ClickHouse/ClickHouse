#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
dba=${db}_a
nodb=${db}_nodb

ON="$CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1"
OFF="$CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=0"

code() { grep -oE 'Code: [0-9]+' | head -n1; }

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${dba}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${dba}"

echo '-- two-part rule: only the dotted table of the current database exists'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`${dba}.b\` (x Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`${dba}.b\` VALUES (1), (2), (3)"
$ON -q "SELECT count() FROM ${dba}.b"
$OFF -q "SELECT count() FROM ${dba}.b" 2>&1 | code

echo '-- two-part rule: both exist'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${dba}.b (x Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${dba}.b VALUES (1), (2), (3), (4), (5)"
$ON -q "SELECT count() FROM ${dba}.b" 2>&1 | code

echo '-- two-part rule: only the table of the other database exists'
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`${dba}.b\`"
$ON -q "SELECT count() FROM ${dba}.b"
$OFF -q "SELECT count() FROM ${dba}.b"

echo '-- two-part rule: neither exists'
$CLICKHOUSE_CLIENT -q "DROP TABLE ${dba}.b"
$ON -q "SELECT count() FROM ${dba}.b" 2>&1 | code

echo '-- two-part rule: the explicit database does not exist'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`${nodb}.b\` (x Int32) ENGINE = Memory"
$ON -q "SELECT count() FROM ${nodb}.b" 2>&1 | code
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`${nodb}.b\`"

echo '-- multipart column qualifiers'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x Int32, y Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (1, 10), (2, 20)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t1\` (x Int32, y Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t1\` VALUES (1, 100), (2, 200)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t2\` (x Int32, y Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t2\` VALUES (2, 2000), (3, 3000)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`a.b\` (c Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`a.b\` VALUES (7)"

$ON -q "SELECT ns.t.x, ${db}.ns.t.y FROM ${db}.ns.t ORDER BY x"
$ON -q "SELECT ns.t.* FROM ${db}.ns.t ORDER BY x"
$ON -q "SELECT a.x FROM ${db}.ns.t AS a ORDER BY a.x"
$ON -q "SELECT ns.t1.x, ns.t2.y FROM ${db}.ns.t1 JOIN ${db}.ns.t2 ON ns.t1.x = ns.t2.x ORDER BY ns.t1.x"
$ON -q "SELECT a.b.c FROM ${db}.\`a.b\`"

echo '-- multipart column qualifiers, setting off'
$OFF -q "SELECT ns.t.x FROM ${db}.ns.t" 2>&1 | code
$OFF -q "SELECT ns.t1.x, ns.t2.y FROM ${db}.ns.t1 JOIN ${db}.ns.t2 ON ns.t1.x = ns.t2.x" 2>&1 | code
$OFF -q "SELECT a.b.c FROM ${db}.\`a.b\`" 2>&1 | code

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t1\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t2\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`a.b\`"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${dba}"
