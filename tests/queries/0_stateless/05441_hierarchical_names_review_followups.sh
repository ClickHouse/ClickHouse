#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed for a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): the names of `joinGet` / `dictGet` in a session whose current
# database is a namespace, and in a mutation; a resolved name is not reinterpreted by the lookup; `DROP ... ON CLUSTER`
# and `EXCHANGE TABLES` bind the names they act on; the `GRANTS` of an authentication method bind the names like `GRANT`;
# `USE` checks the access before reporting a database that does not exist; `merge` reads a namespace.

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function run_as()
{
    $CLICKHOUSE_CLIENT --user "${user}" -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E '\(([A-Z_]+)\)' | head -1
}

run "DROP USER IF EXISTS ${user}"
run "CREATE USER ${user}"

run "CREATE TABLE ${db}.\`ns.j\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`ns.j\` VALUES (1, 'j1')"
run "CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO ${db}.src VALUES (1, 'd1')"
run "CREATE DICTIONARY ${db}.\`ns.d\` (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}')) LAYOUT(FLAT()) LIFETIME(0)"

echo '--- unqualified names of joinGet and dictGet inside USE db.ns are bound to db.ns.j and db.ns.d'
run "USE ${db}.ns; CREATE VIEW ${db}.v AS SELECT joinGet(j, 'v', toUInt64(1)) AS a, joinGet('j', 'v', toUInt64(1)) AS b, dictGet(d, 'v', toUInt64(1)) AS c, dictGet('d', 'v', toUInt64(1)) AS e"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}' AND name = 'v'"
run "SELECT * FROM ${db}.v"

echo '--- a qualified name of joinGet in a mutation is bound to db.ns.j'
run "CREATE TABLE ${db}.m (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO ${db}.m VALUES (1, '')"
run "USE ${db}; ALTER TABLE m UPDATE v = joinGet(ns.j, 'v', k) WHERE 1 SETTINGS mutations_sync = 2"
run "SELECT * FROM ${db}.m"
run "ALTER TABLE ${db}.m UPDATE v = dictGet(ns.d, 'v', k) WHERE 1 SETTINGS mutations_sync = 2"
run "SELECT * FROM ${db}.m"
run "SELECT command FROM system.mutations WHERE database = '${db}' AND table = 'm' ORDER BY mutation_id"

echo '--- the bound names keep naming the same objects when a database db.ns appears'
run "CREATE DATABASE \`${db}.ns\`"
run "CREATE TABLE \`${db}.ns\`.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \`${db}.ns\`.j VALUES (1, 'other')"
run "CREATE TABLE \`${db}.ns\`.src (k UInt64, v String) ENGINE = Memory"
run "INSERT INTO \`${db}.ns\`.src VALUES (1, 'other')"
run "CREATE DICTIONARY \`${db}.ns\`.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}.ns')) LAYOUT(FLAT()) LIFETIME(0)"
run "SELECT * FROM ${db}.v"
run "DROP DATABASE \`${db}.ns\`"

echo '--- a resolved name is not reinterpreted as relative to the current database'
# The database has the table `db.t`, but no table `t`: `db.t` inside `USE db` is the missing table `t`.
run "CREATE TABLE ${db}.\`${db}.t\` (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.\`${db}.t\` VALUES (1)"
run "INSERT INTO ${db}.\`${db}.t\` VALUES (1)"
run "GRANT OPTIMIZE, SHOW COLUMNS ON ${db}.t TO ${user}"
run "GRANT SHOW DATABASES ON ${db}.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -m -q "USE ${db}; OPTIMIZE TABLE ${db}.t FINAL" 2>&1 | error_code
$CLICKHOUSE_CLIENT --user "${user}" -m -q "USE ${db}; DESCRIBE TABLE ${db}.t" 2>&1 | error_code
run "SELECT count() FROM system.parts WHERE database = '${db}' AND table = '${db}.t' AND active"

echo '--- TRUNCATE ON CLUSTER is checked for the table it truncates'
run "CREATE TABLE ${db}.\`ns.t\` (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.\`ns.t\` VALUES (1)"
run "GRANT CLUSTER ON *.* TO ${user}"
run "GRANT TRUNCATE, DROP TABLE ON \`${db}.ns\`.* TO ${user}"
run_as "TRUNCATE TABLE ${db}.ns.t ON CLUSTER test_shard_localhost" | error_code
run_as "DROP TABLE ${db}.ns.t ON CLUSTER test_shard_localhost" | error_code
run "SELECT count() FROM ${db}.\`ns.t\`"
run "TRUNCATE TABLE ${db}.ns.t ON CLUSTER test_shard_localhost FORMAT Null"
run "SELECT count() FROM ${db}.\`ns.t\`"

echo '--- both operands of EXCHANGE TABLES are existing tables'
run "CREATE DATABASE \`${db}.b\`"
run "CREATE TABLE ${db}.\`b.t\` (x String) ENGINE = Memory"
run "INSERT INTO ${db}.\`b.t\` VALUES ('b.t')"
run "CREATE TABLE ${db}.x (x String) ENGINE = Memory"
run "INSERT INTO ${db}.x VALUES ('x')"
run "EXCHANGE TABLES ${db}.x AND ${db}.b.t"
run "SELECT * FROM ${db}.x"
run "SELECT * FROM ${db}.\`b.t\`"
run "DROP DATABASE \`${db}.b\`"

echo '--- the GRANTS of an authentication method bind the names like GRANT'
run "USE ${db}; ALTER USER ${user} IDENTIFIED WITH plaintext_password BY 'p1', plaintext_password BY 'p2' GRANTS (SELECT ON ns.t)"
run "SHOW CREATE USER ${user}" | grep -o 'GRANTS ([^)]*)'

echo '--- USE checks the access before reporting a database that does not exist'
run "REVOKE ALL ON *.* FROM ${user}"
run "ALTER USER ${user} IDENTIFIED WITH no_password"
run_as "USE ${db}_nonexistent" | error_code
run_as "USE ${db}.ns" | error_code
run_as "USE ${db}.nonexistent" | error_code

echo '--- merge reads a namespace'
run "CREATE TABLE ${db}.\`ns.m1\` (x UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`ns.m1\` VALUES (1)"
run "CREATE TABLE ${db}.m1 (x UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.m1 VALUES (2)"
run "USE ${db}.ns; SELECT _table, x FROM merge('^m1\$')"
run "SELECT _table, x FROM merge('${db}.ns', '^m1\$')"
run "USE ${db}.ns; CREATE VIEW ${db}.vm AS SELECT _table, x FROM merge('^m1\$')"
run "SELECT * FROM ${db}.vm"

run "DROP USER ${user}"
