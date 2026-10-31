#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): the lookup of a table given as a separate database and table is
# exact, and the name is resolved before the access is checked; a computed constant name of `joinGet` / `dictGet` in a
# view is bound like a string literal; a short `ATTACH` finds the detached table of a namespace without attached tables.

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E '\(([A-Z_]+)\)' | head -1
}

run "DROP USER IF EXISTS ${user}"
run "CREATE USER ${user}"

echo '--- hasColumnInTable checks the access for the table the name resolves to'
run "CREATE TABLE ${db}.\`ns.t\` (secret UInt8) ENGINE = Memory"
run "GRANT SHOW COLUMNS ON \`${db}.ns\`.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT hasColumnInTable('${db}.ns', 't', 'secret')" 2>&1 | error_code
run "GRANT SHOW COLUMNS ON ${db}.\`ns.t\` TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT hasColumnInTable('${db}.ns', 't', 'secret')" 2>&1
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT hasColumnInTable('${db}', 'ns.t', 'secret')" 2>&1

echo '--- a computed constant name of joinGet and dictGet in a view is bound to the object it denotes now'
run "CREATE TABLE ${db}.\`ns.j\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`ns.j\` VALUES (1, 'original')"
run "CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO ${db}.src VALUES (1, 'original')"
run "CREATE DICTIONARY ${db}.\`ns.d\` (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}')) LAYOUT(FLAT()) LIFETIME(0)"
run "USE ${db}; CREATE VIEW ${db}.v AS SELECT joinGet(concat('ns', '.j'), 'v', toUInt64(1)) AS a, dictGet(concat('ns', '.d'), 'v', toUInt64(1)) AS b"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}' AND name = 'v'"
run "SELECT * FROM ${db}.v"
run "CREATE DATABASE \`${db}.ns\`"
run "CREATE TABLE \`${db}.ns\`.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \`${db}.ns\`.j VALUES (1, 'other')"
run "CREATE TABLE \`${db}.ns\`.src (k UInt64, v String) ENGINE = Memory"
run "INSERT INTO \`${db}.ns\`.src VALUES (1, 'other')"
run "CREATE DICTIONARY \`${db}.ns\`.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}.ns')) LAYOUT(FLAT()) LIFETIME(0)"
run "SELECT * FROM ${db}.v"
run "DROP DATABASE \`${db}.ns\`"

echo '--- a short ATTACH of the last table of a namespace'
run "CREATE TABLE ${db}.\`ns2.t\` (x UInt8) ENGINE = Memory"
run "DETACH TABLE ${db}.ns2.t"
run "ATTACH TABLE ${db}.ns2.t"
run "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'ns2.%'"
run "DETACH TABLE ${db}.ns2.t PERMANENTLY"
run "ATTACH TABLE ${db}.ns2.t"
run "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'ns2.%'"
run "USE ${db}; DETACH TABLE ns2.t; ATTACH TABLE ns2.t"
run "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'ns2.%'"

run "DROP USER ${user}"
