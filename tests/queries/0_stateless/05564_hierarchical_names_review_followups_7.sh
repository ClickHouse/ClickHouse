#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): a lambda argument is not a table in a stored view; a column default
# that may be a subcolumn stays an expression on every binding pass of `CREATE` and `ALTER`; the single-part table name
# of another source keeps qualifying its columns; a stored parameterized view call is bound to the view it denotes; a
# `RENAME` destination may use a namespace created by an earlier element of the same query.

db=$CLICKHOUSE_DATABASE
ns=${db}_ns

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E 'Code: [0-9]+|[A-Z_]{5,}' | head -1
}

run "CREATE TABLE ${db}.\`a.b\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`a.b\` VALUES (2)"

echo '--- a lambda argument is not a table'
run "USE ${db}; SELECT arrayMap(a -> toUInt8(1) IN a.b, [CAST(tuple([toUInt8(1)]), 'Tuple(b Array(UInt8))')])"
run "USE ${db}; CREATE VIEW v_lambda AS SELECT arrayMap(a -> toUInt8(1) IN a.b, [CAST(tuple([toUInt8(1)]), 'Tuple(b Array(UInt8))')]) AS hit"
run "SELECT hit FROM ${db}.v_lambda"

echo '--- a column default that may be a subcolumn'
run "USE ${db}; CREATE TABLE t_create (a Tuple(b Array(UInt8)), hit UInt8 DEFAULT 1 IN a.b) ENGINE = Memory"
run "INSERT INTO ${db}.t_create (a) VALUES (([1]))"
run "SELECT hit FROM ${db}.t_create"
run "CREATE TABLE ${db}.t_alter (k UInt8) ENGINE = MergeTree ORDER BY k"
run "USE ${db}; ALTER TABLE t_alter ADD COLUMN a Tuple(b Array(UInt8)), ADD COLUMN hit UInt8 DEFAULT 1 IN a.b"
run "INSERT INTO ${db}.t_alter (k, a) VALUES (1, ([1]))"
run "SELECT hit FROM ${db}.t_alter"

echo '--- the table name of another source qualifies its columns'
run "CREATE TABLE ${db}.\`q.b\` (c UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.q (b Tuple(c UInt8)) ENGINE = Memory"
run "INSERT INTO ${db}.\`q.b\` VALUES (1)"
run "INSERT INTO ${db}.q VALUES ((2))"
for prefer_left in 0 1; do
    run "SELECT q.b.c FROM ${db}.\`q.b\` CROSS JOIN ${db}.q SETTINGS single_join_prefer_left_table = ${prefer_left}"
    run "SELECT q.b.c FROM ${db}.q CROSS JOIN ${db}.\`q.b\` SETTINGS single_join_prefer_left_table = ${prefer_left}"
done

echo '--- a stored parameterized view call is bound'
run "CREATE VIEW ${db}.\`${ns}.pv\` AS SELECT {x:UInt8} + 1 AS v"
run "USE ${db}; CREATE VIEW v_saved AS SELECT v FROM ${ns}.pv(x = 1)"
run "SELECT * FROM ${db}.v_saved"
run "CREATE DATABASE ${ns}"
run "CREATE VIEW ${ns}.pv AS SELECT {x:UInt8} + 10 AS v"
run "USE ${db}; SELECT * FROM v_saved"
run "USE ${db}; SELECT * FROM ${ns}.pv(x = 1)"
run "DROP DATABASE ${ns}"

echo '--- a rename destination uses a namespace created by an earlier element'
run "CREATE TABLE ${db}.r_x (k UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.r_z (k UInt8) ENGINE = Memory"
run "RENAME TABLE ${db}.r_z TO ${db}.rns.t" | error_code
run "RENAME TABLE ${db}.r_x TO ${db}.\`rns.y\`, ${db}.r_z TO ${db}.rns.t"
run "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'rns.%' ORDER BY name"
