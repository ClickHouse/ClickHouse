#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# The queries of an external database source (`query`, `where`, `update_field`, `invalidate_query`) are sent
# to the database with the credentials of the source. These credentials can come from a named collection that
# the author of the dictionary has no access to, because the dictionary is loaded with the global context.
# So `CREATE DICTIONARY` requires the privilege `NAMED COLLECTION` on the collection it uses, and the queries
# of its source must be `SELECT`. The checks of the queries themselves are in `05872_dictionary_external_source_queries_syntax`,
# `READ ON ODBC` for an ODBC source in `05873_dictionary_odbc_source_read_on_odbc`; this test covers what needs
# a user or a named collection. No database server is needed: the dictionaries are lazy and never loaded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_mysql"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
replicated_db="${CLICKHOUSE_DATABASE}_replicated"

function cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP DICTIONARY IF EXISTS dict_select_only;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc;
        DROP NAMED COLLECTION IF EXISTS ${nc}_secret;
        DROP DATABASE IF EXISTS ${replicated_db} SYNC;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION $nc AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', db = 'd'"

# Prints the number of dictionaries `dict_select_only` in the test database and drops it, in one client.
function exists_and_drop()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict_select_only';
        DROP DICTIONARY IF EXISTS dict_select_only;"
}

echo '--- The queries of a source with a named collection are checked'
$CLICKHOUSE_CLIENT --query "
    CREATE DICTIONARY dict_select_only (id UInt64, value String) PRIMARY KEY id
        SOURCE(MYSQL(NAME $nc TABLE 't' INVALIDATE_QUERY 'UPDATE t SET value = 1 WHERE id = 1'))
        LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1)" 2>&1 \
    | grep -q "Only SELECT query can be used as a dictionary invalidate query" && echo 1 || echo 0
exists_and_drop

MYSQL="MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd'"

echo '--- The source is evaluated once, so the stored query is the checked one'
# `randConstant()` returns a new value at each evaluation. 20 dictionaries are created in one client, each with about one chance
# in two to pass the check. Prints whether any was created, and how many store `DROP TABLE`.
queries=""
for i in {1..20}; do
    queries+="CREATE DICTIONARY dict_random_query_$i (id UInt64, value String) PRIMARY KEY id
        SOURCE($MYSQL QUERY if(randConstant() % 2 = 0, 'SELECT id, value FROM t', 'DROP TABLE victim')))
        LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);"
done
$CLICKHOUSE_CLIENT --ignore-error --multiquery --query "$queries" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0, countIf(create_table_query LIKE '%DROP TABLE victim%')
    FROM system.tables WHERE database = currentDatabase() AND name LIKE 'dict_random_query_%'"

echo '--- A named collection requires the privilege NAMED COLLECTION'
# The backup for `RESTORE` is made once, by `default`. Reading it requires `READ ON DISK`, which is a part of `SOURCES`.
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE USER $user;
    GRANT SOURCES, CLUSTER ON *.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO $user;
    CREATE DICTIONARY dict_select_only (id UInt64, value String) PRIMARY KEY id
        SOURCE(MYSQL(NAME $nc TABLE 't')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
    BACKUP DICTIONARY dict_select_only TO ${backup} FORMAT Null;
    DROP DICTIONARY dict_select_only;"

# Prints whether the `CREATE` was denied for the named collection, and whether the dictionary exists.
# The dictionary is not loaded at `CREATE`, so nothing connects to the source.
function create_as_user()
{
    local on_cluster="$1"
    local name="$2"
    $CLICKHOUSE_CLIENT --user "$user" --distributed_ddl_output_mode=none --multiquery --query "
        CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_select_only ${on_cluster} (id UInt64, value String) PRIMARY KEY id
            SOURCE(MYSQL(NAME ${name} TABLE 't')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);" 2>&1 \
        | grep -qE "necessary to have the grant NAMED COLLECTION ON \`?${nc}\`?\." && echo 1 || echo 0
    exists_and_drop
}

# `RESTORE` creates the dictionary with an internal query, so it requires the privileges of the source itself.
# Restores the backup as the user, `ON CLUSTER`. Prints whether the `RESTORE` was denied for the named collection,
# and whether the dictionary exists.
function restore_as_user()
{
    $CLICKHOUSE_CLIENT --user "$user" --query "
        RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_select_only ON CLUSTER test_shard_localhost FROM ${backup} FORMAT Null" 2>&1 \
        | grep -qE "necessary to have the grant NAMED COLLECTION ON \`?${nc}\`?\." && echo 1 || echo 0
    exists_and_drop
}

echo 'Without the grant: denied, not created'
create_as_user "" "$nc"
echo 'Without the grant, ON CLUSTER: denied, not created'
create_as_user "ON CLUSTER test_shard_localhost" "$nc"
echo 'Without the grant, the name is an expression: denied, not created'
create_as_user "" "concat('$nc', '')"
echo 'Without the grant, RESTORE ON CLUSTER: denied, not restored'
restore_as_user
$CLICKHOUSE_CLIENT --query "GRANT NAMED COLLECTION ON $nc TO $user"
echo 'With the grant: created'
create_as_user "" "$nc"
echo 'With the grant, RESTORE ON CLUSTER: restored'
restore_as_user

echo 'With the grant, the name is an expression: created'
create_as_user "" "concat('$nc', '')"

echo '--- The collection name is evaluated once, so the stored collection is the authorized one'
# A `Replicated` database creates the dictionary with full access, so only the check of the query of the user applies.
# The user has the grant on `$nc` only. Prints whether any dictionary was created, and how many store the other collection.
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE NAMED COLLECTION ${nc}_secret AS host = '127.0.0.1', port = 1, user = 'admin', password = 'secret', db = 'd';
    CREATE DATABASE ${replicated_db} ENGINE = Replicated('/clickhouse/{database}/replicated_db', 'shard_1', 'replica_1');
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${replicated_db}.* TO $user;"
queries=""
for i in {1..20}; do
    queries+="CREATE DICTIONARY ${replicated_db}.dict_random_name_$i (id UInt64, value String) PRIMARY KEY id
        SOURCE(MYSQL(NAME if(randConstant() % 2 = 0, '$nc', '${nc}_secret') TABLE 't'))
        LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);"
done
$CLICKHOUSE_CLIENT --user "$user" --ignore-error --multiquery --query "$queries" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0, countIf(create_table_query LIKE '%${nc}_secret%')
    FROM system.tables WHERE database = '${replicated_db}' AND name LIKE 'dict_random_name_%'"

