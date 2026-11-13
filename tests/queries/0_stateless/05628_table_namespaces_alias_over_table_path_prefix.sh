#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An alias equal to the first component of a multipart table name takes priority over the table path as a column qualifier.

db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.tupletest\` (x UInt64, tupletest Tuple(x UInt64)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.c\` (x UInt64, ns Tuple(c Tuple(x UInt64))) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.m\` (tup Tuple(a UInt64), m Tuple(tup Tuple(b UInt64))) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`${db}.t\` (x UInt64, t Tuple(x UInt64)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.tupletest\` VALUES (1, tuple(2))"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.c\` VALUES (1, tuple(tuple(2)))"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (5)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.m\` VALUES (tuple(1), tuple(tuple(2)))"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`${db}.t\` VALUES (1, tuple(2))"

# Each statement runs with analyzer_compatibility_prefer_alias_over_subcolumn = 0, then = 1.
both()
{
    for compat in 0 1; do
        $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
            --analyzer_compatibility_prefer_alias_over_subcolumn=$compat "$@"
    done
}

echo '-- 1. alias equals the first path component, the alias qualifies the subcolumn'
both -q "SELECT ns.tupletest.x FROM \`ns.tupletest\` AS ns"

echo '-- 2. same in WHERE'
both -q "SELECT count() FROM \`ns.tupletest\` AS ns WHERE ns.tupletest.x = 1"

echo '-- 3. alias equals the database name, table path starts with it'
both -q "SELECT ${db}.t.x FROM ${db}.\`${db}.t\` AS ${db}"

echo '-- 4. longer matcher qualifier'
both -q "SELECT ns.m.tup.* FROM \`ns.m\` AS ns"

echo '-- 5. unrelated alias, the table path still qualifies'
both -q "SELECT ns.tupletest.x FROM \`ns.tupletest\` AS zz"

echo '-- 6. no alias'
both -q "SELECT ns.tupletest.x FROM \`ns.tupletest\`"

echo '-- 7. alias reading fails, the compatibility setting prefers the table path over the subcolumn'
both -q "SELECT ns.c.x FROM \`ns.c\` AS ns"

echo '-- 8. table path is tried after a failed alias lookup'
# Only with the compatibility setting: without it the alias is the only qualifier tried here.
$CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
    --analyzer_compatibility_prefer_alias_over_subcolumn=1 \
    -q "SELECT ns.t.x FROM \`ns.t\` AS ns"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.tupletest\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.c\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.m\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`${db}.t\`"
