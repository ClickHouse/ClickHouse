#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-replicated-database: a `Replicated` database does not allow `ON CLUSTER`.

# A table created as `url(...)` needs `TABLE ENGINE ON URL`, like `ENGINE = URL(...)`, which a filtered source grant
# does not imply. `ON CLUSTER` creates the table on the hosts with full access, so the grant must be required before the
# query is sent to them: with explicit columns, with the legacy entry format, which is not normalized on the initiator,
# and for `CREATE TABLE copy AS src`, which inherits the function of `src`. A local `RESTORE` needs the grant too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_$(random_str 8)"
table="table_$(random_str 8)"
source_table="source_$(random_str 8)"
copy="copy_$(random_str 8)"
restored="restored_$(random_str 8)"
backup="Disk('backups', '${CLICKHOUSE_DATABASE}_$(random_str 8)')"
cluster="ON CLUSTER test_shard_localhost"
url_definition="(x UInt32) AS url('http://localhost:11111/allowed/data.tsv', 'TSV', 'x UInt32')"
copy_definition="AS ${CLICKHOUSE_DATABASE}.${source_table}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.${source_table} ${url_definition};
    BACKUP TABLE ${CLICKHOUSE_DATABASE}.${source_table} TO ${backup} FORMAT Null;
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.*, CLUSTER, CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${source_table} TO ${user};
    GRANT READ ON DISK TO ${user};
    GRANT READ, WRITE ON URL('^http://localhost:11111/allowed/.*$') TO ${user};
"

# Print the header of `scenario`, run `query` as the user with the extra client arguments, and print the error code and
# the missing grant, or `OK`.
function run_as_user()
{
    local scenario=$1 query=$2
    shift 2
    echo "-- ${scenario}"
    local error
    error=$($CLICKHOUSE_CLIENT --user "${user}" --distributed_ddl_output_mode throw "$@" -q "${query}" 2>&1 \
        | grep -o "ACCESS_DENIED\|TABLE ENGINE ON URL" | sort -u | paste -sd ' ')
    echo "${error:-OK}"
}

# The same for `CREATE TABLE <name> <on_cluster> <definition>`.
function create_table()
{
    local scenario=$1 name=$2 on_cluster=$3 definition=$4
    shift 4
    run_as_user "${scenario}" "CREATE TABLE ${CLICKHOUSE_DATABASE}.${name} ${on_cluster} ${definition}" "$@"
}

# The same for restoring the source table as `restored`.
function restore_table()
{
    local scenario=$1
    run_as_user "${scenario}" \
        "RESTORE TABLE ${CLICKHOUSE_DATABASE}.${source_table} AS ${CLICKHOUSE_DATABASE}.${restored} FROM ${backup} FORMAT Null"
}

function count_tables()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('${table}', '${copy}', '${restored}')"
}

create_table "local" "${table}" "" "${url_definition}"
create_table "on cluster" "${table}" "${cluster}" "${url_definition}"
create_table "on cluster, format 2" "${table}" "${cluster}" "${url_definition}" --distributed_ddl_entry_format_version 2
create_table "copy" "${copy}" "" "${copy_definition}"
create_table "copy, on cluster" "${copy}" "${cluster}" "${copy_definition}"
create_table "copy, format 2" "${copy}" "${cluster}" "${copy_definition}" --distributed_ddl_entry_format_version 2
restore_table "restore"
echo "-- no table"
count_tables

# Positive control: with the grant, the same queries are allowed, so the refusals above come from the missing grant.
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON URL TO ${user}"
create_table "on cluster, granted" "${table}" "${cluster}" "${url_definition}"
create_table "copy, granted" "${copy}" "" "${copy_definition}"
restore_table "restore, granted"
echo "-- tables created"
count_tables

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
