#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table created `AS view(...)` with a column list creates its storage on first use, with the global context. The query
# of `view` must be evaluated only by the query that reads the table, as its user, so a dictionary the user cannot access
# must not be loaded.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster_db="${CLICKHOUSE_DATABASE}_on_cluster"
allowed="d_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -m --query "
        DROP DATABASE IF EXISTS ${cluster_db};
        DROP USER IF EXISTS ${username};
    "
}
cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -m --query "
    CREATE TABLE src (id UInt64, value String) ENGINE = Memory;
    INSERT INTO src VALUES (1, 'a');
    CREATE DICTIONARY d (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE DICTIONARY ${allowed} (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE DATABASE ${cluster_db};
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
    GRANT dictGet ON ${CLICKHOUSE_DATABASE}.${allowed} TO ${username};
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${username};
    GRANT CREATE TABLE ON ${cluster_db}.* TO ${username};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src TO ${username};
    GRANT CLUSTER ON *.* TO ${username};
"

function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"
}

function status()
{
    query "SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd'"
}

function unload()
{
    query "SYSTEM UNLOAD DICTIONARY d"
}

# Runs a query as the user, with the given extra URL parameters.
function as_user()
{
    echo "$1" | sed -e "s/${username}/user/g" -e "s/${cluster_db}/cluster_db/g" -e "s/${allowed}/d_allowed/g" -e "s/${CLICKHOUSE_DATABASE}/db/g"
    local output
    output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${username}&distributed_ddl_output_mode=none${2:-}" --data-binary "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}" | sed -e "s/${cluster_db}/cluster_db/g" -e "s/${CLICKHOUSE_DATABASE}/db/g"
    fi
}

echo "--- dictGet on a dictionary the user cannot access"
unload
as_user "CREATE TABLE w_view (value String) AS view(SELECT dictGet('${CLICKHOUSE_DATABASE}.d', 'value', toUInt64(1)) AS value)"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.w_view TO ${username}"
as_user "SELECT * FROM w_view"
status
unload
as_user "CREATE TABLE ${cluster_db}.w_view ON CLUSTER test_shard_localhost (value String) AS view(SELECT dictGet('${CLICKHOUSE_DATABASE}.d', 'value', toUInt64(1)) AS value)" "&distributed_ddl_entry_format_version=2"
query "GRANT SELECT ON ${cluster_db}.w_view TO ${username}"
as_user "SELECT * FROM ${cluster_db}.w_view"
status

echo "--- a dictionary name that depends on the context"
# The name evaluates to an allowed dictionary for the user, but to `d` in the global context.
unload
as_user "CREATE TABLE w_context (value String) AS view(SELECT dictGet(if(currentUser() = '${username}', '${CLICKHOUSE_DATABASE}.${allowed}', '${CLICKHOUSE_DATABASE}.d'), 'value', toUInt64(1)) AS value)"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.w_context TO ${username}"
as_user "SELECT * FROM w_context"
status

echo "--- a table that does not exist where the table is created, e.g. on the initiator of ON CLUSTER"
as_user "CREATE TABLE w_missing (x UInt64) AS view(SELECT x FROM ${CLICKHOUSE_DATABASE}.missing_table)"
as_user "CREATE TABLE ${cluster_db}.w_missing ON CLUSTER test_shard_localhost (x UInt64) AS view(SELECT x FROM ${CLICKHOUSE_DATABASE}.missing_table)"
query "SELECT count() FROM system.tables WHERE (database, name) IN (('${CLICKHOUSE_DATABASE}', 'w_missing'), ('${cluster_db}', 'w_missing'))"

echo "--- a Nullable column returned through a JOIN is converted to the declared type"
query "CREATE TABLE w_join (value String) AS view(SELECT rhs.value AS value FROM ${CLICKHOUSE_DATABASE}.src AS lhs LEFT JOIN ${CLICKHOUSE_DATABASE}.src AS rhs USING (id) SETTINGS join_use_nulls = 1)"
query "SELECT * FROM w_join"
# A row without a match has `NULL`, which the declared type cannot hold.
query "CREATE TABLE src_right (id UInt64, value String) ENGINE = Memory"
query "INSERT INTO src_right VALUES (2, 'b')"
query "CREATE TABLE w_join_null (value String) AS view(SELECT rhs.value AS value FROM ${CLICKHOUSE_DATABASE}.src AS lhs LEFT JOIN ${CLICKHOUSE_DATABASE}.src_right AS rhs USING (id) SETTINGS join_use_nulls = 1)"
query "SELECT * FROM w_join_null" 2>&1 | grep -o -m1 "(CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN)\|(INCORRECT_QUERY)" || echo "no error"
