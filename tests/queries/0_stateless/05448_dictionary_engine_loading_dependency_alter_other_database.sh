#!/usr/bin/env bash
# Regression test: a metadata-only `ALTER` of a table in another database must keep resolving an unqualified argument
# of the `Dictionary` table engine against the current database of the session, as `CREATE` does.
# Otherwise the loading dependency moves to the database of the table and the dictionary can be dropped while the table
# still reads from it.
#
# The tables are attached because `CREATE` would check that the dictionary exists in the default database of the server
# rather than in the current database of the session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

ATOMIC_DB="${CLICKHOUSE_DATABASE}_atomic"
MEMORY_DB="${CLICKHOUSE_DATABASE}_memory"
TABLE_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ATOMIC_DB}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${MEMORY_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${ATOMIC_DB} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${MEMORY_DB} ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY loading_dep_d (id UInt64, value String)
    PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    ATTACH TABLE ${ATOMIC_DB}.loading_dep_t UUID '${TABLE_UUID}' (id UInt64, value String)
    ENGINE = Dictionary('loading_dep_d')"
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    ATTACH TABLE ${MEMORY_DB}.loading_dep_t (id UInt64, value String)
    ENGINE = Dictionary('loading_dep_d')"

$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ATOMIC_DB}.loading_dep_t MODIFY COMMENT 'touch'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${MEMORY_DB}.loading_dep_t MODIFY COMMENT 'touch'"

$CLICKHOUSE_CLIENT -q "
    SELECT database = '${ATOMIC_DB}' ? 'atomic' : 'memory', loading_dependencies_database = ['${CLICKHOUSE_DATABASE}']
    FROM system.tables
    WHERE database IN ('${ATOMIC_DB}', '${MEMORY_DB}') AND name = 'loading_dep_t'
    ORDER BY database"

$CLICKHOUSE_CLIENT -q "
    DROP DICTIONARY loading_dep_d
    SETTINGS check_table_dependencies = 1, check_referential_table_dependencies = 0" 2>&1 | grep -o -m1 "HAVE_DEPENDENT_OBJECTS"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${ATOMIC_DB}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${MEMORY_DB}"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY loading_dep_d"
