#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL and PostgreSQL integrations are not available in the fast test build.

# The queries of an external database source (`query`, `where`, `update_field`, `invalidate_query`) are sent
# to the database with the credentials of the source. These credentials can come from a named collection that
# the author of the dictionary has no access to, because the dictionary is loaded with the global context.
# So `CREATE DICTIONARY` requires the privilege `NAMED COLLECTION` on the collection it uses, and the queries
# of its source must be `SELECT`, without what the database may read differently: comments, heredocs, typographic
# quotes and backslashes.
#
# No database server is needed: the queries are checked by `CREATE`, before any connection. The sources point
# at `127.0.0.1` port 1, where nothing listens, so an accepted source ends in a connection failure at
# `SYSTEM RELOAD DICTIONARY`. That failure is asserted too: it proves that the source can be created.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_mysql"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"

NOT_SELECT="Only SELECT query can be used as a dictionary"
DIFFERENT_READING="cannot contain comments, heredocs, typographic quotes or backslashes"
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

function dictionary_exists()
{
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict_select_only'"
}

# Runs the statement that creates the dictionary `dict_select_only` with the source, and checks that it is rejected
# with the expected error and that no dictionary is left.
function check_rejected()
{
    local description="$1"
    local create="$2"
    local source="$3"
    local expected="$4"
    local output
    output=$($CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "
        ${create} (id UInt64, value String) PRIMARY KEY id
            SOURCE(${source}) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1)" 2>&1)
    if echo "$output" | grep -q "$expected" && [ "$(dictionary_exists)" = "0" ]; then
        echo "$description: OK"
    else
        echo "$description: FAIL, expected an error matching '$expected' and no dictionary, got: $output"
    fi
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_select_only"
}

# Creates the dictionary, and checks that it is created and that loading it fails only to connect.
function check_accepted()
{
    local description="$1"
    local source="$2"
    local output
    output=$($CLICKHOUSE_CLIENT --multiquery --query "
        CREATE DICTIONARY dict_select_only (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
            SOURCE(${source}) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
        SYSTEM RELOAD DICTIONARY dict_select_only;" 2>&1)
    if echo "$output" | grep -q "$CONNECTION_FAILED" && [ "$(dictionary_exists)" = "1" ]; then
        echo "$description: OK"
    else
        echo "$description: FAIL, expected a created dictionary and a connection failure, got: $output"
    fi
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_select_only"
}

CREATE="CREATE DICTIONARY dict_select_only"
MYSQL="MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd'"
POSTGRESQL="POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd'"

echo '--- Statements other than SELECT are rejected'
check_rejected "MySQL invalidate_query" "$CREATE" "$MYSQL TABLE 't' INVALIDATE_QUERY 'CREATE TABLE t2 (x UInt8) ENGINE = Memory')" "$NOT_SELECT invalidate query"
check_rejected "MySQL query" "$CREATE" "$MYSQL QUERY 'DROP TABLE t')" "$NOT_SELECT source"
check_rejected "MySQL query with INTO OUTFILE" "$CREATE" "$MYSQL QUERY 'SELECT id, value FROM t INTO OUTFILE ''/tmp/x''')" "$NOT_SELECT source"
check_rejected "MySQL where" "$CREATE" "$MYSQL TABLE 't' WHERE '1; DROP TABLE t')" "Multi-statements are not allowed"
check_rejected "MySQL update_field" "$CREATE" "$MYSQL TABLE 't' UPDATE_FIELD 'x >= 0; DROP TABLE t')" "Multi-statements are not allowed"
check_rejected "MySQL named collection invalidate_query" "$CREATE" "MYSQL(NAME $nc TABLE 't' INVALIDATE_QUERY 'UPDATE t SET value = 1 WHERE id = 1')" "$NOT_SELECT invalidate query"
check_rejected "PostgreSQL invalidate_query" "$CREATE" "$POSTGRESQL TABLE 't' INVALIDATE_QUERY 'DROP TABLE t')" "$NOT_SELECT invalidate query"
check_rejected "PostgreSQL query" "$CREATE" "$POSTGRESQL QUERY 'DELETE FROM t WHERE id = 1')" "$NOT_SELECT source"
check_rejected "MySQL query is an expression" "$CREATE" "$MYSQL QUERY concat('DROP', ' TABLE t'))" "$NOT_SELECT source"
check_rejected "ODBC invalidate_query" "$CREATE" "ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'DROP TABLE t')" "$NOT_SELECT invalidate query"

echo '--- What the database reads differently from ClickHouse is rejected'
check_rejected "MySQL executable comment" "$CREATE" "$MYSQL TABLE 't' INVALIDATE_QUERY '/*!CREATE TABLE t2 AS */ SELECT 1')" "$DIFFERENT_READING"
check_rejected "MySQL -- without a space" "$CREATE" "$MYSQL TABLE 't' INVALIDATE_QUERY 'SELECT 1 --1 INTO OUTFILE ''/tmp/x''')" "$DIFFERENT_READING"
check_rejected "MySQL comment in where" "$CREATE" "$MYSQL TABLE 't' WHERE 'id > 0 /*!*/')" "$DIFFERENT_READING"
check_rejected "Heredoc" "$CREATE" "$MYSQL QUERY 'SELECT \$\$a\$\$ AS id, value FROM t')" "$DIFFERENT_READING"
check_rejected "Typographic quotes" "$CREATE" "$MYSQL TABLE 't' INVALIDATE_QUERY 'SELECT 1 AS ‘, 2 INTO OUTFILE ''/tmp/x'' ’')" "$DIFFERENT_READING"
check_rejected "PostgreSQL backslash" "$CREATE" "$POSTGRESQL TABLE 't' UPDATE_FIELD 'value = ''\\\\'' OR updated')" "$DIFFERENT_READING"

echo '--- Every way to create a dictionary from a definition is checked'
check_rejected "ON CLUSTER" "$CREATE ON CLUSTER test_shard_localhost" "$MYSQL QUERY 'DROP TABLE t')" "$NOT_SELECT source"
check_rejected "CREATE OR REPLACE" "CREATE OR REPLACE DICTIONARY dict_select_only" "$MYSQL QUERY 'DROP TABLE t')" "$NOT_SELECT source"

echo '--- SELECT is accepted'
check_accepted "MySQL" "$MYSQL QUERY 'SELECT id, value FROM t WHERE {condition}' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t')"
check_accepted "MySQL with expressions" "MYSQL(HOST concat('127.0.0.1', '') PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY concat('SELECT ', '1'))"
check_accepted "PostgreSQL" "$POSTGRESQL TABLE 't' WHERE 'id > 0' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t')"

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
    dictionary_exists
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
echo 'With the grant, the name is an expression: created'
create_as_user "" "concat('$nc', '')"
