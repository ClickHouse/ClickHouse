#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# The queries of a MySQL, PostgreSQL or ODBC dictionary source are sent to the database with the credentials of the source,
# which can come from a named collection that the author of the dictionary has no grant for: the dictionary is loaded
# with the global context. So a dictionary created by `CREATE DICTIONARY` sends only `SELECT`, and `CREATE DICTIONARY`
# requires `NAMED COLLECTION` on the collection of its source. The checks of the load query are in
# `05872_dictionary_external_source_queries_syntax`.

# The MySQL source connects to this server, and a failed self-connect attempt is reported at Warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
backend_user="${CLICKHOUSE_TEST_UNIQUE_NAME}_backend"
nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"

function cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP DICTIONARY IF EXISTS dict_invalidate;
        DROP DICTIONARY IF EXISTS dict_nc;
        DROP USER IF EXISTS $user;
        DROP USER IF EXISTS $backend_user;
        DROP NAMED COLLECTION IF EXISTS $nc;"
}
trap cleanup EXIT
cleanup

echo '--- A non-SELECT invalidate query is not sent'
# The source connects to this server over the MySQL protocol as a user that can create tables, so the canary table
# would be created if the invalidate query were sent. The invalidate query runs when the lifetime expires: the failed
# check is logged by the loader, and the dictionary is updated anyway.
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE USER $backend_user IDENTIFIED WITH plaintext_password BY 'p';
    GRANT SELECT, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $backend_user;
    CREATE TABLE src (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO src VALUES (1, 'a');
    CREATE DICTIONARY dict_invalidate (id UInt64, value String) PRIMARY KEY id
        SOURCE(MYSQL(HOST '127.0.0.1' PORT ${CLICKHOUSE_PORT_MYSQL} USER '$backend_user' PASSWORD 'p' DB '${CLICKHOUSE_DATABASE}' TABLE 'src'
            INVALIDATE_QUERY 'CREATE TABLE ${CLICKHOUSE_DATABASE}.canary (x UInt8) ENGINE = Memory'))
        LAYOUT(FLAT()) LIFETIME(MIN 1 MAX 1);
    SELECT dictGet(dict_invalidate, 'value', toUInt64(1));"

for _ in {1..120}; do
    rejected=$($CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() > 0 FROM system.text_log
        WHERE event_date >= yesterday()
            AND message LIKE '%Could not check if dictionary%${CLICKHOUSE_DATABASE}.dict_invalidate%Only SELECT query can be used as a dictionary invalidate query%'")
    [ "$rejected" = "1" ] && break
    sleep 0.5
done
echo "rejected: $rejected"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'canary'"

echo '--- A named collection requires the privilege NAMED COLLECTION'
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE NAMED COLLECTION $nc AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', db = 'd';
    CREATE USER $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT CLUSTER ON *.* TO $user;"

# Prints whether the `CREATE` was denied for the named collection, and whether the dictionary exists.
function create_as_user()
{
    local on_cluster="$1"
    local name="$2"
    $CLICKHOUSE_CLIENT --user "$user" --distributed_ddl_output_mode=none --query "
        CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_nc ${on_cluster} (id UInt64, value String) PRIMARY KEY id
            SOURCE(MYSQL(NAME ${name} TABLE 't')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1)" 2>&1 \
        | grep -qE "necessary to have the grant NAMED COLLECTION ON \`?${nc}\`?\." && echo 1 || echo 0
    $CLICKHOUSE_CLIENT --multiquery --query "
        SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict_nc';
        DROP DICTIONARY IF EXISTS dict_nc;"
}

echo 'Without the grant: denied, not created'
create_as_user "" "$nc"
echo 'Without the grant, ON CLUSTER: denied, not created'
create_as_user "ON CLUSTER test_shard_localhost" "$nc"
echo 'Without the grant, the name is an expression: denied, not created'
create_as_user "" "concat('$nc', '')"
$CLICKHOUSE_CLIENT --query "GRANT NAMED COLLECTION ON $nc TO $user"
echo 'With the grant: created'
create_as_user "" "$nc"
echo 'With the grant, ON CLUSTER: created'
create_as_user "ON CLUSTER test_shard_localhost" "$nc"
