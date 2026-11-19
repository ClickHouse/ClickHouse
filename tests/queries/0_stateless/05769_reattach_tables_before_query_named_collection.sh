#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Tag no-replicated-database: tables of `Replicated` databases are not reattached.
# Tag no-ordinary-database: the test finds the internal `DETACH TABLE ... SYNC` in the log by the UUID of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `reattach_tables_before_query_execution_probability` runs `DETACH` and `ATTACH` with full access,
# so it must not reattach a table whose engine takes its configuration from a named collection
# which the user has no access to: the user could not `ATTACH` such a table itself.

user="user_05769_${CLICKHOUSE_DATABASE}"
collection="collection_05769_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS $collection;
    CREATE NAMED COLLECTION $collection AS url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', format = 'TSV';
    CREATE TABLE t_url (x UInt64) ENGINE = URL($collection);
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, DROP VIEW, CREATE TABLE, CREATE VIEW ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT TABLE ENGINE ON URL TO $user;
"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_url'")

function count_detaches()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND message = 'Waiting for detached table $uuid to be no longer in use'"
}

$CLICKHOUSE_CLIENT --user "$user" -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
echo "user without NAMED COLLECTION: $(count_detaches) detaches"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_url"

$CLICKHOUSE_CLIENT -q "GRANT NAMED COLLECTION ON $collection TO $user"

$CLICKHOUSE_CLIENT --user "$user" -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
echo "user with NAMED COLLECTION: $(count_detaches) detaches"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_url"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_url;
    DROP USER $user;
    DROP NAMED COLLECTION $collection;
"
