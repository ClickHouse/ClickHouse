#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: Replicated database does not support `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): a string naming a table of a database with dots is stored in the
# quoted form, so that the dependencies see it; a compound name in the right argument of `IN` that may be a subcolumn is
# not bound to a table; the current database is applied once to an unqualified name; a table name of several parts does
# not take precedence over the alias of another source with `analyzer_compatibility_prefer_alias_over_subcolumn`; the
# tables of a standalone mutation and of `CREATE TABLE ... AS SELECT` are bound before the query is dispatched
# `ON CLUSTER`. The current database is the database of the test, so unqualified names are relative to it.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E 'Code: [0-9]+|[A-Z_]{5,}' | head -1
}

echo '--- joinGet of a table of a database with dots is a dependency of the view'
run "CREATE DATABASE \`${db}_r.ns\`"
run "CREATE TABLE \`${db}_r.ns\`.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \`${db}_r.ns\`.j VALUES (1, 'joined')"
run "CREATE VIEW ${db}.v_join AS SELECT joinGet('${db}_r.ns.j', 'v', toUInt64(1)) AS v"
run "SELECT v FROM ${db}.v_join"
run "SELECT position(create_table_query, '\`${db}_r.ns\`.j') > 0 FROM system.tables WHERE database = '${db}' AND name = 'v_join'"
run "DROP TABLE \`${db}_r.ns\`.j SETTINGS check_referential_table_dependencies = 1" | error_code
run "DROP TABLE ${db}.v_join"
run "DROP DATABASE \`${db}_r.ns\`"

echo '--- a compound right argument of IN that may be a subcolumn is not bound to a table'
run "CREATE TABLE ${db}.t_in (a Tuple(b Array(UInt8))) ENGINE = Memory"
run "INSERT INTO ${db}.t_in SELECT tuple([toUInt8(1)])"
run "CREATE TABLE ${db}.\`a.b\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`a.b\` VALUES (2)"
run "SELECT 1 IN a.b AS hit FROM t_in"
run "CREATE VIEW ${db}.v_in AS SELECT 1 IN a.b AS hit FROM t_in"
run "SELECT hit FROM ${db}.v_in"
echo '--- the same name without such a column is the table'
run "SELECT 2 IN a.b AS hit"
run "CREATE VIEW ${db}.v_in_table AS SELECT 2 IN a.b AS hit"
run "SELECT hit FROM ${db}.v_in_table"

echo '--- the current database is applied once to an unqualified name'
run "CREATE DATABASE \`${db}_x.y\`"
run "CREATE TABLE \`${db}_x.y\`.\`${db}_x.y.t\` (v UInt8) ENGINE = Memory"
run "USE \`${db}_x.y\`; EXISTS TABLE t"
run "USE \`${db}_x.y\`; SELECT * FROM t" | error_code
run "USE \`${db}_x.y\`; DROP TABLE IF EXISTS t"
run "SELECT count() FROM system.tables WHERE database = '${db}_x.y'"
run "DROP DATABASE \`${db}_x.y\`"

echo '--- a table name of several parts does not take precedence over an alias, also in the compatibility mode'
run "CREATE TABLE ${db}.\`p.q\` (c UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.t_join (q Tuple(c UInt8)) ENGINE = Memory"
run "INSERT INTO ${db}.\`p.q\` VALUES (1)"
run "INSERT INTO ${db}.t_join VALUES ((2))"
run "SELECT p.q.c FROM \`p.q\` CROSS JOIN t_join AS p SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1"

echo '--- the tables of a standalone mutation and of CREATE TABLE AS SELECT are bound before ON CLUSTER'
run "CREATE TABLE ${db}.\`ns.keys\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`ns.keys\` VALUES (1)"
run "CREATE TABLE ${db}.\`ns.src\` (x UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`ns.src\` VALUES (5)"
run "CREATE TABLE ${db}.dst (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.dst VALUES (1), (2)"
$CLICKHOUSE_CLIENT --mutations_sync=2 -q "DELETE FROM ${db}.dst ON CLUSTER test_shard_localhost WHERE x IN ${db}.ns.keys" 2>&1 | error_code
run "SELECT x FROM ${db}.dst ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.copy ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT * FROM ${db}.ns.src FORMAT Null" 2>&1 | error_code
run "SELECT x FROM ${db}.copy"
