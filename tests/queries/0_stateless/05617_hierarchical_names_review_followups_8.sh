#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag no-replicated-database: Replicated database does not support `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): a materialized CTE qualifies its columns before a longer
# hierarchical table name; the columns of a subquery source decide whether a compound name is an expression or a
# table; the `SELECT` of `CREATE TABLE ... AS SELECT` is bound before a `Replicated` database enqueues it; an `ALTER`
# command validated before `ON CLUSTER` keeps the subcolumns of the mutated table as expressions.

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

echo '--- a materialized CTE qualifies its columns'
run "CREATE TABLE ${db}.\`a.b\` (c UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`a.b\` VALUES (1)"
for prefer_left in 0 1; do
    run "WITH a AS MATERIALIZED (SELECT 2 AS \`b.c\`) SELECT a.b.c FROM ${db}.\`a.b\` CROSS JOIN a SETTINGS enable_materialized_cte = 1, single_join_prefer_left_table = ${prefer_left}"
done

echo '--- the columns of a subquery source'
run "CREATE TABLE ${db}.\`p.q\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`p.q\` VALUES (1, 'other')"
run "USE ${db}; CREATE VIEW v_join_expression AS SELECT joinGet(p.q, 'v', toUInt64(1)) AS v FROM (SELECT CAST(tuple('j_one'), 'Tuple(q String)') AS p)" | error_code
run "USE ${db}; CREATE VIEW v_join_table AS SELECT joinGet(p.q, 'v', toUInt64(1)) AS v FROM (SELECT 1 AS x)"
run "SELECT * FROM ${db}.v_join_table"
run "CREATE TABLE ${db}.\`${ns}.s\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`${ns}.s\` VALUES (1)"
run "USE ${db}; CREATE VIEW v_in AS SELECT 1 IN ${ns}.s AS hit FROM (SELECT 1 AS x)"
run "CREATE DATABASE ${ns}"
run "CREATE TABLE ${ns}.s (x UInt8) ENGINE = Set"
run "SELECT hit FROM ${db}.v_in"
run "DROP DATABASE ${ns}"

echo '--- CREATE AS SELECT in a Replicated database'
run "CREATE DATABASE \`${db}_repl\` ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db}_repl', 's1', 'r1')"
run "SET distributed_ddl_output_mode = 'none'; CREATE TABLE \`${db}_repl\`.\`ns.src\` (x UInt8) ENGINE = Memory"
run "INSERT INTO \`${db}_repl\`.\`ns.src\` VALUES (5)"
run "SET distributed_ddl_output_mode = 'none'; CREATE TABLE \`${db}_repl\`.copy ENGINE = Memory AS SELECT * FROM ${db}_repl.ns.src"
run "SELECT * FROM \`${db}_repl\`.copy"
run "DROP DATABASE \`${db}_repl\` SYNC"

echo '--- ALTER ON CLUSTER with a subcolumn on the right side of IN'
run "CREATE TABLE ${db}.t (m Tuple(n Array(UInt8)), hit UInt8) ENGINE = MergeTree ORDER BY tuple()"
run "INSERT INTO ${db}.t VALUES (([1]), 0)"
run "ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost UPDATE hit = 1 IN m.n WHERE 1 SETTINGS mutations_sync = 2, distributed_ddl_output_mode = 'none'"
run "SELECT hit FROM ${db}.t"
