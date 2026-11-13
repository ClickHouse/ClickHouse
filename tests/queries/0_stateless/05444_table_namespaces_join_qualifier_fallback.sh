#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The database-qualified fallback must match multipart table names when a JOIN alias equals the database name.

db=${CLICKHOUSE_DATABASE}

code() { grep -oE 'Code: [0-9]+' | head -n1; }

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.sub.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.u (y UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (7)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.sub.t\` VALUES (9)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.u VALUES (8)"

# Each statement runs with analyzer_compatibility_prefer_alias_over_subcolumn = 0, then = 1.
both()
{
    for compat in 0 1; do
        $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
            --analyzer_compatibility_prefer_alias_over_subcolumn=$compat "$@"
    done
}

echo '-- 1. left side, alias equals the database name'
both -q "SELECT ${db}.ns.t.x FROM ${db}.ns.t INNER JOIN ${db}.u AS ${db} ON 1"

echo '-- 2. right side, alias equals the database name'
both -q "SELECT ${db}.ns.t.x FROM ${db}.u AS ${db} INNER JOIN ${db}.ns.t ON 1"

echo '-- 3. three-part table name'
both -q "SELECT ${db}.ns.sub.t.x FROM ${db}.ns.sub.t INNER JOIN ${db}.u AS ${db} ON 1"

echo '-- 4. matcher'
both -q "SELECT ${db}.ns.t.* FROM ${db}.ns.t INNER JOIN ${db}.u AS ${db} ON 1"

echo '-- 5. missing column'
for compat in 0 1; do
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
        --analyzer_compatibility_prefer_alias_over_subcolumn=$compat \
        -q "SELECT ${db}.ns.t.nope FROM ${db}.ns.t INNER JOIN ${db}.u AS ${db} ON 1" 2>&1 | code
done

echo '-- 6. both sides multipart, alias equals the database name'
both -q "SELECT ${db}.ns.t.x, ${db}.ns.sub.t.x FROM ${db}.ns.t INNER JOIN ${db}.ns.sub.t AS ${db} ON 1"

echo '-- 7. setting off'
$CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=0 \
    -q "SELECT ${db}.ns.t.x FROM ${db}.ns.t INNER JOIN ${db}.u AS ${db} ON 1" 2>&1 | code

echo '-- 8. SEMI JOIN, non-preserved multipart side in a dead branch'
for compat in 0 1; do
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --allow_experimental_table_namespaces=1 \
        --analyzer_compatibility_prefer_alias_over_subcolumn=$compat --semi_join_include_columns_from_both_sides=0 \
        -q "SELECT if(false, ${db}.ns.t.x, 42) FROM ${db}.u LEFT SEMI JOIN ${db}.ns.t ON true" 2>&1 | code
done

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.sub.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.u"
