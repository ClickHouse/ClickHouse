#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: DDL in a `Replicated` database is executed with a context without the user, so it is not checked

# An `ALTER` of a column used by an existing projection rebuilds that projection, and the rebuilt projection
# is executed in the background, so an AI function reaching it through an `ALIAS` is rejected even with the privilege.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="u_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $USER;
    CREATE USER $USER IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, INSERT, SELECT, ALTER ON ${CLICKHOUSE_DATABASE}.* TO $USER;
    GRANT TABLE ENGINE ON MergeTree TO $USER;
    GRANT AI FUNCTIONS ON *.* TO $USER;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (x String, y String ALIAS x, PROJECTION p (SELECT x WHERE y != '' ORDER BY x)) ENGINE = MergeTree ORDER BY x;
"

$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.t MODIFY COLUMN y String ALIAS aiGenerate(x)" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "SELECT create_table_query LIKE '%aiGenerate%' FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 't'"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE ${CLICKHOUSE_DATABASE}.t;
    DROP USER $USER;
"
