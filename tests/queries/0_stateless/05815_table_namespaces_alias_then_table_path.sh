#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the first part of an identifier is both a table alias and the first component of the table path, the table path is tried after the alias.

db=${CLICKHOUSE_DATABASE}
db2=${CLICKHOUSE_DATABASE}_2

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db2}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db2}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.u (y UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`${db}.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`${db2}.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db2}.t (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (5)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db2}.\`ns.t\` VALUES (6)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.u VALUES (7)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`${db}.t\` VALUES (3)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`${db2}.t\` VALUES (4)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db2}.t VALUES (9)"

# Each statement runs with analyzer_compatibility_prefer_alias_over_subcolumn = 0, then = 1.
# The correlated subqueries and single_join_prefer_left_table settings are needed by cases 10-11.
both()
{
    for compat in 0 1; do
        $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
            --allow_experimental_correlated_subqueries=1 --single_join_prefer_left_table=1 \
            --analyzer_compatibility_prefer_alias_over_subcolumn=$compat "$@"
    done
}

echo '-- 1. alias equals the first path component'
both -q "SELECT ns.t.x FROM ${db}.\`ns.t\` AS ns"

echo '-- 2. same in WHERE'
both -q "SELECT count() FROM ${db}.\`ns.t\` AS ns WHERE ns.t.x = 5"

echo '-- 3. with a JOIN'
both -q "SELECT ns.t.x FROM ${db}.\`ns.t\` AS ns JOIN ${db}.u AS u ON 1"

echo '-- 4. in JOIN ON'
both -q "SELECT count() FROM ${db}.\`ns.t\` AS ns JOIN ${db}.u AS u ON ns.t.x + 2 = u.y"

echo '-- 5. alias equals the database name'
both -q "SELECT ${db}.t.x FROM ${db}.\`${db}.t\` AS ${db}"

echo '-- 6. same table path on both sides of a JOIN, alias on the left'
both -q "SELECT ns.t.x FROM ${db}.\`ns.t\` AS ns JOIN ${db2}.\`ns.t\` AS b ON 1"

echo '-- 7. alias on the right'
both -q "SELECT ns.t.x FROM ${db2}.\`ns.t\` AS b JOIN ${db}.\`ns.t\` AS ns ON 1"

echo '-- 8. comma join'
both -q "SELECT ns.t.x FROM ${db}.\`ns.t\` AS ns, ${db2}.\`ns.t\` AS b"

echo '-- 9. column through the alias and through the path'
both -q "SELECT ns.x, ns.t.x FROM ${db}.\`ns.t\` AS ns JOIN ${db2}.\`ns.t\` AS b ON 1"

echo '-- 10. alias equals the database name, the other side is a table of that database'
both -q "SELECT ${db2}.t.x FROM ${db}.\`${db2}.t\` AS ${db2} JOIN ${db2}.t ON 1"

echo '-- 11. the alias does not outrank a database-qualified reading of the other side, so the left side wins without the compatibility setting'
both -q "SELECT ${db2}.t.x FROM ${db2}.t JOIN ${db}.\`${db2}.t\` AS ${db2} ON 1"

echo '-- 12. in a scalar subquery'
both -q "SELECT (SELECT ${db2}.t.x FROM ${db}.\`${db2}.t\` AS ${db2} LIMIT 1) FROM ${db2}.t"

echo '-- 13. a missing column is still an error'
for compat in 0 1; do
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
        --analyzer_compatibility_prefer_alias_over_subcolumn=$compat \
        -q "SELECT ns.t.nope FROM ${db}.\`ns.t\` AS ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
done

echo '-- 14. without table namespaces the path is not tried'
for compat in 0 1; do
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=0 \
        --analyzer_compatibility_prefer_alias_over_subcolumn=$compat \
        -q "SELECT ns.t.x FROM ${db}.\`ns.t\` AS ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
done

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db2}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.u"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`${db}.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`${db2}.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db2}.t"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db2}"
