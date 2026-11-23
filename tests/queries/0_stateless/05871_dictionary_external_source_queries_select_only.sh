#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL and PostgreSQL integrations are not available in the fast test build.

# The queries of an external database source (`query`, `where`, `update_field`, `invalidate_query`) are sent
# to the database verbatim, with the credentials of the source. These credentials can come from a named
# collection that the author of the dictionary has no access to, because the dictionary is loaded with the
# global context. So a dictionary created with a DDL query requires the privilege `NAMED COLLECTION` on the
# collection it uses, and the queries of its source must be `SELECT`.
#
# No database server is needed: the queries are checked while the source is instantiated, before any
# connection. The sources point at `127.0.0.1` port 1, where nothing listens, so an accepted source ends in
# a connection failure. That failure is asserted too: it proves that the source was not rejected for some
# unrelated reason.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_mysql"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"

NOT_SELECT="Only SELECT query can be used as a dictionary"
CONNECTION_FAILED="ALL_CONNECTION_TRIES_FAILED\|POSTGRESQL_CONNECTION_FAILURE"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_select_only"
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $user"
    $CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS $nc"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION $nc AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', db = 'd'"

# Creates and loads the dictionary, and checks the error.
function check()
{
    local description="$1"
    local source="$2"
    local expected="$3"
    local output
    output=$($CLICKHOUSE_CLIENT --multiquery --query "
        DROP DICTIONARY IF EXISTS dict_select_only;
        CREATE DICTIONARY dict_select_only (id UInt64, value String) PRIMARY KEY id
            SOURCE(${source}) LAYOUT(FLAT()) LIFETIME(0);
        SYSTEM RELOAD DICTIONARY dict_select_only;" 2>&1)
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_select_only"
    if echo "$output" | grep -q "$expected"; then
        echo "$description: OK"
    else
        echo "$description: FAIL, expected an error matching '$expected', got: $output"
    fi
}

MYSQL="MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd'"
POSTGRESQL="POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd'"

echo '--- Statements other than SELECT are rejected'
check "MySQL invalidate_query" "$MYSQL TABLE 't' INVALIDATE_QUERY 'CREATE TABLE t2 (x UInt8) ENGINE = Memory')" "$NOT_SELECT invalidate query"
check "MySQL query" "$MYSQL QUERY 'DROP TABLE t')" "$NOT_SELECT source"
check "MySQL query with INTO OUTFILE" "$MYSQL QUERY 'SELECT id, value FROM t INTO OUTFILE ''/tmp/x''')" "$NOT_SELECT source"
check "MySQL where" "$MYSQL TABLE 't' WHERE '1; DROP TABLE t')" "Multi-statements are not allowed"
check "MySQL update_field" "$MYSQL TABLE 't' UPDATE_FIELD 'x >= 0; DROP TABLE t')" "Multi-statements are not allowed"
check "MySQL named collection invalidate_query" "MYSQL(NAME $nc TABLE 't' INVALIDATE_QUERY 'UPDATE t SET value = 1 WHERE id = 1')" "$NOT_SELECT invalidate query"
check "PostgreSQL invalidate_query" "$POSTGRESQL TABLE 't' INVALIDATE_QUERY 'DROP TABLE t')" "$NOT_SELECT invalidate query"
check "PostgreSQL query" "$POSTGRESQL QUERY 'DELETE FROM t WHERE id = 1')" "$NOT_SELECT source"

echo '--- SELECT is accepted'
check "MySQL" "$MYSQL QUERY 'SELECT id, value FROM t WHERE {condition}' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t')" "$CONNECTION_FAILED"
check "PostgreSQL" "$POSTGRESQL TABLE 't' WHERE 'id > 0' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t')" "$CONNECTION_FAILED"

echo '--- A named collection requires the privilege NAMED COLLECTION'
$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE USER $user;
    GRANT SOURCES, CLUSTER ON *.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO $user;"

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
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict_select_only'"
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_select_only"
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
