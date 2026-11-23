#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-replicated-database: a `Replicated` database does not allow `ON CLUSTER`.

# `filesystem` has no table engine, so a table created as `filesystem(...)` needs `READ` and `WRITE` on `FILE`, the
# grants that imply `TABLE ENGINE ON File`. With explicit columns the structure is not inferred, and `ON CLUSTER`
# creates the table on the hosts with full access, so the grants must be required before the query is sent to them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
echo -n 'canary' > "${CLICKHOUSE_USER_FILES_UNIQUE}/victim.txt"

user="user_${CLICKHOUSE_DATABASE}_$(random_str 8)"
table="table_$(random_str 8)"
source_table="source_$(random_str 8)"
copy="copy_$(random_str 8)"
file_table="file_$(random_str 8)"
cluster="ON CLUSTER test_shard_localhost"
definition="(name String, content Nullable(String)) AS filesystem('${CLICKHOUSE_TEST_UNIQUE_NAME}')"
copy_definition="AS ${CLICKHOUSE_DATABASE}.${source_table}"
file_definition="(line String) AS file('${CLICKHOUSE_TEST_UNIQUE_NAME}/victim.txt', 'LineAsString')"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.${source_table} ${definition};
    CREATE USER ${user};
    GRANT CREATE TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.*, CLUSTER, CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${source_table} TO ${user};
"

# Print the header of `scenario`, run `query` as the user, and print the error code and the missing grant, or `OK`.
function run_as_user()
{
    local scenario=$1 query=$2
    echo "-- ${scenario}"
    local error
    error=$($CLICKHOUSE_CLIENT --user "${user}" --distributed_ddl_output_mode throw -q "${query}" 2>&1 \
        | grep -o "ACCESS_DENIED\|TABLE ENGINE ON File" | sort -u | paste -sd ' ')
    echo "${error:-OK}"
}

# The same for `CREATE TABLE <name> <on_cluster> <definition>`.
function create_table()
{
    local scenario=$1 name=$2 on_cluster=$3 definition=$4
    run_as_user "${scenario}" "CREATE TABLE ${CLICKHOUSE_DATABASE}.${name} ${on_cluster} ${definition}"
}

$CLICKHOUSE_CLIENT -q "GRANT WRITE ON FILE TO ${user}"
echo "-- WRITE ON FILE"
create_table "local" "${table}" "" "${definition}"
create_table "on cluster" "${table}" "${cluster}" "${definition}"
create_table "copy" "${copy}" "" "${copy_definition}"
create_table "file" "${file_table}" "" "${file_definition}"

$CLICKHOUSE_CLIENT -q "REVOKE WRITE ON FILE FROM ${user}; GRANT READ ON FILE TO ${user}"
echo "-- READ ON FILE"
create_table "local" "${table}" "" "${definition}"
create_table "on cluster" "${table}" "${cluster}" "${definition}"

# Positive control: with both grants, the same queries are allowed, so the refusals above come from the missing grant.
$CLICKHOUSE_CLIENT -q "GRANT WRITE ON FILE TO ${user}"
echo "-- READ, WRITE ON FILE"
create_table "on cluster" "${table}" "${cluster}" "${definition}"
create_table "copy" "${copy}" "" "${copy_definition}"
echo "-- content"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT content FROM ${CLICKHOUSE_DATABASE}.${table} WHERE name = 'victim.txt'"
echo "-- tables created"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('${table}', '${copy}')"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
