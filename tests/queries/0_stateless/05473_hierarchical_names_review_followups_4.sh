#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag no-ordinary-database: UNDROP needs an Atomic database.
# Tag no-replicated-database: Replicated database does not support UNDROP and `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): the source of `ATTACH PARTITION ... FROM` left unqualified is
# authorized as the table that is actually read; `UNDROP TABLE` and a short `ATTACH TABLE` try the name as written in
# the current database first; a table name of several parts does not take precedence over the alias or the exact
# database and table name of another source of a join; the table operands of a mutation are bound before the query
# is dispatched `ON CLUSTER`. The current database is the database of the test, so unqualified names are relative to it.

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

echo '--- ATTACH PARTITION FROM an unqualified source checks SELECT on the table actually read'
run "CREATE TABLE ${db}.dst (x UInt8) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE ${db}.\`${db}.b.c\` (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.\`${db}.b.c\` VALUES (1)"
run "DROP USER IF EXISTS ${user}"
run "CREATE USER ${user}"
run "GRANT INSERT ON ${db}.dst TO ${user}"
run "GRANT SELECT ON ${db}.\`b.c\` TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "ALTER TABLE dst ATTACH PARTITION ALL FROM \`b.c\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'
run "SELECT count() FROM ${db}.dst"
run "DROP USER ${user}"

echo '--- UNDROP TABLE restores the table of the current database named as written'
run "CREATE DATABASE \`${db}.ns\`"
run "CREATE TABLE ${db}.\`ns.t\` (v UInt8) ENGINE = MergeTree ORDER BY v"
run "CREATE TABLE \`${db}.ns\`.t (v UInt8) ENGINE = MergeTree ORDER BY v"
run "INSERT INTO ${db}.\`ns.t\` VALUES (1)"
run "INSERT INTO \`${db}.ns\`.t VALUES (2)"
run "DROP TABLE ${db}.\`ns.t\`"
run "DROP TABLE \`${db}.ns\`.t"
run "UNDROP TABLE \`ns.t\`"
run "SELECT 'db.ns.t', v FROM ${db}.\`ns.t\`"
run "SELECT 'db.ns', count() FROM system.tables WHERE database = '${db}.ns' AND name = 't'"
run "UNDROP TABLE \`${db}.ns\`.t"

echo '--- a short ATTACH TABLE attaches the table of the current database named as written'
run "DETACH TABLE ${db}.\`ns.t\`"
run "DETACH TABLE \`${db}.ns\`.t"
run "ATTACH TABLE \`ns.t\`"
run "SELECT 'db.ns.t', v FROM ${db}.\`ns.t\`"
run "SELECT 'db.ns', count() FROM system.tables WHERE database = '${db}.ns' AND name = 't'"
run "ATTACH TABLE \`${db}.ns\`.t"
run "SELECT 'db.ns', v FROM \`${db}.ns\`.t"

echo '--- a table name of several parts does not take precedence over the alias of another source'
run "CREATE TABLE ${db}.\`a.b\` (c UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.t (b Tuple(c UInt8)) ENGINE = Memory"
run "INSERT INTO ${db}.\`a.b\` VALUES (1)"
run "INSERT INTO ${db}.t VALUES ((2))"
run "SELECT a.b.c FROM \`a.b\` CROSS JOIN t AS a"
run "SELECT a.b.c FROM \`a.b\` CROSS JOIN t AS a SETTINGS single_join_prefer_left_table = 0"
run "SELECT a.b.c FROM \`a.b\` INNER JOIN t AS a ON 1"
run "SELECT a.b.* FROM \`a.b\` CROSS JOIN t AS a"

echo '--- a table name of several parts does not take precedence over the exact database and table name of another source'
run "CREATE DATABASE ${db}_a"
run "CREATE TABLE ${db}_a.b (id UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.\`${db}_a.b\` (id UInt8) ENGINE = Memory"
run "INSERT INTO ${db}_a.b VALUES (3)"
run "INSERT INTO ${db}.\`${db}_a.b\` VALUES (4)"
run "SELECT ${db}_a.b.id FROM \`${db}_a.b\` CROSS JOIN ${db}_a.b"
run "SELECT ${db}_a.b.id FROM \`${db}_a.b\` INNER JOIN ${db}_a.b ON 1"
run "SELECT ${db}_a.b.* FROM \`${db}_a.b\` CROSS JOIN ${db}_a.b"
run "DROP DATABASE ${db}_a"

echo '--- the table operand of a mutation is bound before ON CLUSTER'
run "CREATE TABLE ${db}.\`ns.src\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`ns.src\` VALUES (1)"
run "CREATE TABLE ${db}.mutated (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.mutated VALUES (1), (2)"
$CLICKHOUSE_CLIENT --mutations_sync=2 -q "ALTER TABLE ${db}.mutated ON CLUSTER test_shard_localhost DELETE WHERE x IN ${db}.ns.src" 2>&1 | grep -o -m1 -E 'Code: [0-9]+|UNKNOWN_[A-Z]+' 
run "SELECT x FROM ${db}.mutated"
run "SELECT count() FROM system.mutations WHERE database = '${db}' AND table = 'mutated' AND latest_fail_reason != ''"

run "DROP DATABASE \`${db}.ns\`"
