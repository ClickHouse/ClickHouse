#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the ODBC integration is not available in the fast test build.

# An ODBC dictionary source can use the credentials of a DSN configured on the server, as the `odbc` table function,
# so `CREATE DICTIONARY` and `RESTORE` of such a dictionary require `READ ON ODBC`. A source replaced by `Null` sends nothing,
# so it requires nothing. The dictionaries are lazy and never loaded, so no ODBC bridge is needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A separate user without `SOURCES`: a replicated access storage stores the grants in the format of the old versions,
# which has no separate `READ` and `WRITE` on a source, so a partial revoke of `SOURCES` would not be kept.
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP DICTIONARY IF EXISTS dict_odbc;
        DROP USER IF EXISTS $user;"
}
trap cleanup EXIT
cleanup

# The backup is made once, by `default`, and restored by every case. Reading it requires `READ ON DISK`.
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE USER $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT READ ON DISK TO $user;
    GRANT CLUSTER ON *.* TO $user;
    CREATE DICTIONARY dict_odbc (id UInt64, value String) PRIMARY KEY id
        SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
    BACKUP DICTIONARY dict_odbc TO ${backup} FORMAT Null;
    DROP DICTIONARY dict_odbc;"

# Prints whether the source is `NULL` (empty if there is no dictionary), and the number of dictionaries, then drops it.
function check_and_drop()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT count(), countIf(create_table_query LIKE '%SOURCE(NULL%')
        FROM system.tables WHERE database = currentDatabase() AND name = 'dict_odbc';
        DROP DICTIONARY IF EXISTS dict_odbc;"
}

# Prints whether the `CREATE` was denied for `READ ON ODBC`.
function create_as_user()
{
    $CLICKHOUSE_CLIENT --user "$user" --query "
        CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_odbc (id UInt64, value String) PRIMARY KEY id
            SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1)" 2>&1 \
        | grep -q "necessary to have the grant READ ON ODBC" && echo 1 || echo 0
    check_and_drop
}

# Prints whether the `RESTORE` was denied for `READ ON ODBC`.
function restore_as_user()
{
    local on_cluster="$1"
    local settings="$2"
    local options="${3:-}"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user "$user" $options --query "
        RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_odbc ${on_cluster} FROM ${backup} ${settings} FORMAT Null" 2>&1 \
        | grep -q "necessary to have the grant READ ON ODBC" && echo 1 || echo 0
    check_and_drop
}

echo 'Without the grant: denied, not created'
create_as_user
echo 'Without the grant, RESTORE: denied, not restored'
restore_as_user "" ""
echo 'Without the grant, RESTORE replacing the source by Null: restored'
restore_as_user "" "SETTINGS restore_replace_external_dictionary_source_to_null = 1"
echo 'Without the grant, RESTORE replacing the source by Null with the setting of the session: restored'
restore_as_user "" "" "--restore_replace_external_dictionary_source_to_null=1"
echo 'Without the grant, RESTORE ON CLUSTER replacing the source by Null: restored'
restore_as_user "ON CLUSTER test_shard_localhost" "SETTINGS restore_replace_external_dictionary_source_to_null = 1"
$CLICKHOUSE_CLIENT --query "GRANT READ ON ODBC TO $user"
echo 'With the grant: created'
create_as_user
echo 'With the grant, RESTORE: restored'
restore_as_user "" ""
