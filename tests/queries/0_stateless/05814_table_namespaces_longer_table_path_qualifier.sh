#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A qualifier that is a longer table path than the table it is resolved against must resolve to the longer table.

db=${CLICKHOUSE_DATABASE}

code() { grep -oE 'Code: [0-9]+' | head -n1; }

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.ns (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t.deep\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`m.t\` (x UInt64, deep Tuple(x UInt64)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`m.t.deep\` (y UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.ns VALUES (5)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t.deep\` VALUES (2)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`m.t\` VALUES (1, tuple(10))"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`m.t.deep\` VALUES (2)"

# Each statement runs with analyzer_compatibility_prefer_alias_over_subcolumn = 0, then = 1.
both()
{
    for compat in 0 1; do
        $CLICKHOUSE_CLIENT --enable_analyzer=1 --enable_table_namespaces=1 --allow_experimental_correlated_subqueries=1 \
            --analyzer_compatibility_prefer_alias_over_subcolumn=$compat "$@"
    done
}

echo '-- 1. qualifier is a longer table path, left side'
both -q "SELECT ns.t.deep.x FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1"

echo '-- 2. qualifier is a longer table path, right side'
both -q "SELECT ns.t.deep.x FROM ${db}.ns.t.deep JOIN ${db}.ns.t ON 1"

echo '-- 3. comma join'
both -q "SELECT ns.t.deep.x FROM ${db}.ns.t, ${db}.ns.t.deep"

echo '-- 4. database-qualified'
both -q "SELECT ${db}.ns.t.deep.x FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1"

echo '-- 5. matcher'
both -q "SELECT ns.t.deep.* FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1"

echo '-- 6. correlated subquery'
both -q "SELECT (SELECT ns.t.deep.x FROM ${db}.ns.t LIMIT 1) FROM ${db}.ns.t.deep"

echo '-- 7. JOIN ON'
both -q "SELECT count() FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON ns.t.x + 1 = ns.t.deep.x"

echo '-- 8. WHERE'
both -q "SELECT count() FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1 WHERE ns.t.deep.x = 2"

echo '-- 9. shorter table path as qualifier'
both -q "SELECT ns.t.x FROM ${db}.ns JOIN ${db}.ns.t ON 1"

echo '-- 10. missing column in a longer table path'
for compat in 0 1; do
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --enable_table_namespaces=1 --allow_experimental_correlated_subqueries=1 \
        --analyzer_compatibility_prefer_alias_over_subcolumn=$compat \
        -q "SELECT ns.t.deep.nope FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1" 2>&1 | code
done

echo '-- 11. qualifier is the shorter table path'
both -q "SELECT ns.t.x FROM ${db}.ns.t JOIN ${db}.ns.t.deep ON 1"

echo '-- 12. subcolumn when the longer table path has no such column'
both -q "SELECT m.t.deep.x FROM ${db}.m.t JOIN ${db}.m.t.deep ON 1"
