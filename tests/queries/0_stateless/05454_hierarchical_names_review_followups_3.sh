#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a Replicated database does not support renaming several tables in one query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): a later element of a multi-table `RENAME` sees the tables moved
# by the earlier ones; `RENAME TABLE IF EXISTS` of an absent table does nothing; an exact qualifier is preferred over a
# quoted one crossing the boundary between the database and the table in a qualified asterisk; the argument of the
# `Dictionary` engine is bound to the dictionary it denotes.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E '\(([A-Z_]+)\)' | head -1
}

run "CREATE DATABASE \`${db}.b\`"

echo '--- a later element of a multi-table RENAME sees the tables moved by the earlier ones'
run "CREATE TABLE ${db}.x (v UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.\`b.t\` (v UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.x VALUES (1)"
run "INSERT INTO ${db}.\`b.t\` VALUES (2)"
run "RENAME TABLE ${db}.x TO \`${db}.b\`.t, \`${db}.b\`.t TO ${db}.z"
run "SELECT 'z', v FROM ${db}.z"
run "SELECT 'b.t', v FROM ${db}.\`b.t\`"
run "EXISTS TABLE \`${db}.b\`.t"
run "DROP TABLE ${db}.z"
run "DROP TABLE ${db}.\`b.t\`"

echo '--- RENAME TABLE IF EXISTS of an absent table does nothing'
run "RENAME TABLE IF EXISTS ${db}.missing TO ${db}_missing_database.t"
run "RENAME TABLE ${db}.missing TO ${db}_missing_database.t" | error_code

echo '--- an exact qualifier is preferred over a quoted one crossing the boundary in a qualified asterisk'
run "CREATE TABLE ${db}.\`b.c\` (id UInt8) ENGINE = Memory"
run "CREATE TABLE \`${db}.b\`.c (id UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`b.c\` VALUES (1)"
run "INSERT INTO \`${db}.b\`.c VALUES (2)"
run "SELECT \`${db}.b\`.c.* FROM ${db}.\`b.c\` CROSS JOIN \`${db}.b\`.c"
run "SELECT ${db}.\`b.c\`.* FROM ${db}.\`b.c\` CROSS JOIN \`${db}.b\`.c"
run "SELECT \`${db}.b\`.c.* FROM \`${db}.b\`.c CROSS JOIN ${db}.\`b.c\`"
run "DROP TABLE ${db}.\`b.c\`"
run "DROP TABLE \`${db}.b\`.c"

echo '--- the argument of the Dictionary engine is bound to the dictionary it denotes'
run "CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = Memory"
run "INSERT INTO ${db}.src VALUES (1, 'original')"
run "CREATE DICTIONARY ${db}.\`ns.d\` (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}')) LAYOUT(FLAT()) LIFETIME(0)"
run "CREATE TABLE ${db}.t_dict (k UInt64, v String) ENGINE = Dictionary('${db}.ns.d')"
run "SELECT engine_full FROM system.tables WHERE database = '${db}' AND name = 't_dict'"
run "SELECT v FROM ${db}.t_dict"
run "USE ${db}; CREATE TABLE ${db}.t_dict_relative (k UInt64, v String) ENGINE = Dictionary('ns.d')"
run "SELECT engine_full FROM system.tables WHERE database = '${db}' AND name = 't_dict_relative'"
run "DROP DICTIONARY ${db}.\`ns.d\` SETTINGS check_referential_table_dependencies = 1" | error_code

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
run "BACKUP DATABASE ${db} TO ${backup} FORMAT Null"
run "RESTORE DATABASE ${db} AS \`${db}_copy\` FROM ${backup} FORMAT Null"
run "SELECT engine_full FROM system.tables WHERE database = '${db}_copy' AND name = 't_dict'"
run "DROP TABLE ${db}.t_dict"
run "DROP TABLE ${db}.t_dict_relative"
run "DROP DICTIONARY ${db}.\`ns.d\`"
run "TRUNCATE TABLE \`${db}_copy\`.src"
run "INSERT INTO \`${db}_copy\`.src VALUES (1, 'restored')"
run "SYSTEM RELOAD DICTIONARY \`${db}_copy\`.\`ns.d\`"
run "SELECT v FROM \`${db}_copy\`.t_dict"
run "DROP DATABASE \`${db}_copy\` SYNC"

run "DROP DATABASE \`${db}.b\`"
