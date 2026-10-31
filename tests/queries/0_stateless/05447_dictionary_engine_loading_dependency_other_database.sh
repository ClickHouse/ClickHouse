#!/usr/bin/env bash
# Regression test: an unqualified argument of the `Dictionary` table engine is resolved against the current database
# of the session, so the loading dependency of a table created in another database must point there too.
# Otherwise the dictionary can be dropped while the table still reads from it.
#
# This is a shell test because the table is attached with an explicit UUID: `CREATE` would check that the dictionary
# exists in the default database of the server rather than in the current database of the session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

OTHER_DB="${CLICKHOUSE_DATABASE}_other"
TABLE_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${OTHER_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${OTHER_DB}"

$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY loading_dep_d (id UInt64, value String)
    PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    ATTACH TABLE ${OTHER_DB}.loading_dep_t UUID '${TABLE_UUID}' (id UInt64, value String)
    ENGINE = Dictionary('loading_dep_d')"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${OTHER_DB}.loading_dep_t"

$CLICKHOUSE_CLIENT -q "
    DROP DICTIONARY loading_dep_d
    SETTINGS check_table_dependencies = 1, check_referential_table_dependencies = 0" 2>&1 | grep -o -m1 "HAVE_DEPENDENT_OBJECTS"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${OTHER_DB}.loading_dep_t"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${OTHER_DB}"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY loading_dep_d"
