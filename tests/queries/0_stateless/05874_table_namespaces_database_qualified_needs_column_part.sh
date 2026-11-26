#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `db.table` qualifies a column only if a part of the identifier is left for the column.

db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.v (y UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.u (z UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.v VALUES (2)"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.u VALUES (3)"

# Prints the result or the error code; the first argument is analyzer_compatibility_prefer_alias_over_subcolumn.
run()
{
    $CLICKHOUSE_CLIENT --enable_table_namespaces=1 --semi_join_include_columns_from_both_sides=0 \
        --single_join_prefer_left_table=1 --analyzer_compatibility_prefer_alias_over_subcolumn="$1" \
        -q "$2" 2>&1 | grep -oE '^[0-9]+$|Code: [0-9]+' | head -n1
}

echo '-- 1. without a column part it is not a reference to the skipped side'
run 0 "SELECT if(0, ${db}.ns.t, 1) FROM ${db}.v LEFT SEMI JOIN ${db}.\`ns.t\` ON 1"

echo '-- 2. with a column part it is'
run 0 "SELECT if(0, ${db}.ns.t.x, 1) FROM ${db}.v LEFT SEMI JOIN ${db}.\`ns.t\` ON 1"

echo '-- 3. a qualified matcher is looked up by the whole table name'
run 0 "SELECT ${db}.ns.t.* FROM ${db}.v LEFT SEMI JOIN ${db}.\`ns.t\` ON 1"

echo '-- 4. outside a dead branch the identifier without a column part is unknown'
run 0 "SELECT ${db}.ns.t FROM ${db}.v LEFT SEMI JOIN ${db}.\`ns.t\` ON 1"

echo '-- 5. a database-qualified column is found when the alias of the other side is the database name'
run 1 "SELECT ${db}.ns.t.x FROM ${db}.\`ns.t\` AS o JOIN ${db}.u AS ${db} ON 1"

echo '-- 6. and is unknown without a column part'
run 1 "SELECT ${db}.ns.t FROM ${db}.\`ns.t\` AS o JOIN ${db}.u AS ${db} ON 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.v"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.u"
