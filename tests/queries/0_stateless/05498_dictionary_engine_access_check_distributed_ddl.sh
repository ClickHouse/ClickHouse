#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Creating a `Dictionary` table needs `dictGet` or `SELECT` on the dictionary. `ON CLUSTER` and `Replicated` databases
# execute `CREATE` again with full access, so the access must be checked before, and the dictionary must not be loaded.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster_db="${CLICKHOUSE_DATABASE}_on_cluster"
# A dictionary with this name exists both in the test database and in `default`.
unqualified="d_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -m --query "
        DROP DATABASE IF EXISTS ${cluster_db};
        DROP DICTIONARY IF EXISTS default.${unqualified};
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
    CREATE DICTIONARY ${unqualified} (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE DICTIONARY default.${unqualified} (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE DATABASE ${cluster_db};
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
    GRANT dictGet ON ${CLICKHOUSE_DATABASE}.${unqualified} TO ${username};
    GRANT CREATE TABLE, CREATE VIEW ON ${CLICKHOUSE_DATABASE}.* TO ${username};
    GRANT CREATE TABLE ON ${cluster_db}.* TO ${username};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src TO ${username};
    GRANT CLUSTER ON *.* TO ${username};
    GRANT TABLE ENGINE ON Dictionary TO ${username};
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
    echo "$1" | sed -e "s/${username}/user/g" -e "s/${cluster_db}/cluster_db/g" -e "s/${unqualified}/d_unqualified/g" -e "s/${CLICKHOUSE_DATABASE}/db/g"
    local output
    output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${username}&distributed_ddl_output_mode=none${2:-}" --data-binary "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}" | sed -e "s/${cluster_db}/cluster_db/g" -e "s/${CLICKHOUSE_DATABASE}/db/g"
    fi
}

echo "--- ON CLUSTER"
for version in 1 2 ""
do
    unload
    as_user "CREATE TABLE ${cluster_db}.w_${version:-default} ON CLUSTER test_shard_localhost (id UInt64, value String) ENGINE = Dictionary(${CLICKHOUSE_DATABASE}.d)" "${version:+&distributed_ddl_entry_format_version=${version}}"
    status
done

echo "--- inner table of a materialized view"
unload
as_user "CREATE MATERIALIZED VIEW mv ENGINE = Dictionary(${CLICKHOUSE_DATABASE}.d) AS SELECT id, value FROM ${CLICKHOUSE_DATABASE}.src"
status

echo "--- table function"
unload
as_user "CREATE TABLE w_function (id UInt64, value String) AS dictionary('${CLICKHOUSE_DATABASE}.d')"
status
query "EXISTS TABLE w_function"
unload
as_user "CREATE TABLE ${cluster_db}.w_function ON CLUSTER test_shard_localhost (id UInt64, value String) AS dictionary('${CLICKHOUSE_DATABASE}.d')" "&distributed_ddl_entry_format_version=2"
status
query "EXISTS TABLE ${cluster_db}.w_function"
# A table function with an explicit column list creates its storage on first use, with the global context. The query of
# `view` and the table function in `loop` are evaluated only by the query that reads the table, as its user.
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
unload
as_user "CREATE TABLE w_loop (id UInt64, value String) AS loop(dictionary('${CLICKHOUSE_DATABASE}.d'))"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.w_loop TO ${username}"
as_user "SELECT * FROM w_loop LIMIT 1"
status
unload
as_user "CREATE TABLE w_loop_inferred AS loop(dictionary('${CLICKHOUSE_DATABASE}.d'))"
status
# A view over a table that does not exist where the table is created, e.g. on the initiator of `ON CLUSTER`.
as_user "CREATE TABLE w_missing (x UInt64) AS view(SELECT x FROM ${CLICKHOUSE_DATABASE}.missing_table)"
as_user "CREATE TABLE ${cluster_db}.w_missing ON CLUSTER test_shard_localhost (x UInt64) AS view(SELECT x FROM ${CLICKHOUSE_DATABASE}.missing_table)"
query "SELECT count() FROM system.tables WHERE (database, name) IN (('${CLICKHOUSE_DATABASE}', 'w_missing'), ('${cluster_db}', 'w_missing'))"
# A dictionary name that depends on the context evaluates to an allowed dictionary for the user, but would evaluate to `d`
# in the global context, so the query of `view` must not be analyzed with it when the storage is created.
unload
as_user "CREATE TABLE w_context (value String) AS view(SELECT dictGet(if(currentUser() = '${username}', '${CLICKHOUSE_DATABASE}.${unqualified}', '${CLICKHOUSE_DATABASE}.d'), 'value', toUInt64(1)) AS value)"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.w_context TO ${username}"
as_user "SELECT * FROM w_context"
status

echo "--- unqualified name, resolved by ON CLUSTER in the default database of the server"
query "SYSTEM UNLOAD DICTIONARY default.${unqualified}"
as_user "CREATE TABLE ${cluster_db}.w_unqualified ON CLUSTER test_shard_localhost (id UInt64, value String) ENGINE = Dictionary('${unqualified}')"
query "SELECT status FROM system.dictionaries WHERE database = 'default' AND name = '${unqualified}'"

echo "--- dictionary name given by an expression"
unload
as_user "CREATE TABLE w_expression (id UInt64, value String) ENGINE = Dictionary(concat(currentDatabase(), '.d'))"
status
query "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.d TO ${username}"
as_user "CREATE TABLE ${cluster_db}.w_expression ON CLUSTER test_shard_localhost (id UInt64, value String) ENGINE = Dictionary(concat(currentDatabase(), '.d'))"
# The query sent to the replicas carries the checked name, so a replica does not evaluate the expression again.
query "
    SELECT query LIKE '%ENGINE = Dictionary(\\'${CLICKHOUSE_DATABASE}.d\\')%'
    FROM system.distributed_ddl_queue
    WHERE cluster = 'test_shard_localhost' AND query LIKE '%${cluster_db}.w_expression%'
    LIMIT 1"
