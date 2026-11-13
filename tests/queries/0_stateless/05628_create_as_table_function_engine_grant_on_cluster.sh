#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-replicated-database: a `Replicated` database does not allow `ON CLUSTER`.

# A table created as `url(...)` needs `TABLE ENGINE ON URL`, like `ENGINE = URL(...)`, which a filtered source grant
# does not imply. `ON CLUSTER` and `RESTORE ... ON CLUSTER` create the table on the hosts with full access, so the grant
# must be required before the query is sent to them: with explicit columns, with the legacy entry format, which is not
# normalized on the initiator, and for `CREATE TABLE copy AS src`, which inherits the function of `src`. A restore with
# `restore_replace_external_table_functions_to_null` creates a `Null` table, so it needs `TABLE ENGINE ON Null` and must
# not resolve the original function, whose named collection may not exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_$(random_str 8)"
table="table_$(random_str 8)"
source_table="source_$(random_str 8)"
copy="copy_$(random_str 8)"
restored="restored_$(random_str 8)"
collection_source="collection_source_$(random_str 8)"
restored_null="restored_null_$(random_str 8)"
collection="collection_${CLICKHOUSE_DATABASE}_$(random_str 8)"
backup="Disk('backups', '${CLICKHOUSE_DATABASE}_$(random_str 8)')"
collection_backup="Disk('backups', '${CLICKHOUSE_DATABASE}_$(random_str 8)')"
cluster="ON CLUSTER test_shard_localhost"
url_definition="(x UInt32) AS url('http://localhost:11111/allowed/data.tsv', 'TSV', 'x UInt32')"
copy_definition="AS ${CLICKHOUSE_DATABASE}.${source_table}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.${source_table} ${url_definition};
    BACKUP TABLE ${CLICKHOUSE_DATABASE}.${source_table} TO ${backup} FORMAT Null;
    CREATE NAMED COLLECTION ${collection} AS url = 'http://localhost:11111/allowed/data.tsv';
    CREATE TABLE ${CLICKHOUSE_DATABASE}.${collection_source} (x UInt32)
        AS url(${collection}, format = 'TSV', structure = 'x UInt32');
    BACKUP TABLE ${CLICKHOUSE_DATABASE}.${collection_source} TO ${collection_backup} FORMAT Null;
    DROP TABLE ${CLICKHOUSE_DATABASE}.${collection_source};
    DROP NAMED COLLECTION ${collection};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.*, CLUSTER, CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${source_table} TO ${user};
    GRANT READ ON DISK TO ${user};
    GRANT READ, WRITE ON URL('^http://localhost:11111/allowed/.*$') TO ${user};
"

# Print the header of `scenario`, run `query` as the user with the extra client arguments, and print the error code and
# the missing grant, or `OK`. The message quotes `Null`, which is a keyword, with backquotes.
function run_as_user()
{
    local scenario=$1 query=$2
    shift 2
    echo "-- ${scenario}"
    local error
    error=$($CLICKHOUSE_CLIENT --user "${user}" --distributed_ddl_output_mode throw "$@" -q "${query}" 2>&1 \
        | grep -o "ACCESS_DENIED\|TABLE ENGINE ON \`\?[A-Za-z]*\|NAMED_COLLECTION_DOESNT_EXIST" | tr -d '\`' | sort -u | paste -sd ' ')
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
    local scenario=$1 on_cluster=$2
    shift 2
    run_as_user "${scenario}" \
        "RESTORE TABLE ${CLICKHOUSE_DATABASE}.${source_table} AS ${CLICKHOUSE_DATABASE}.${restored} ${on_cluster} FROM ${backup} FORMAT Null" "$@"
}

# The same for restoring the table that used the dropped named collection `ON CLUSTER` as a `Null` table.
function restore_to_null()
{
    local scenario=$1
    run_as_user "${scenario}" \
        "RESTORE TABLE ${CLICKHOUSE_DATABASE}.${collection_source} AS ${CLICKHOUSE_DATABASE}.${restored_null} ${cluster}
         FROM ${collection_backup} SETTINGS restore_replace_external_table_functions_to_null = 1 FORMAT Null"
}

function count_tables()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('${table}', '${copy}', '${restored}', '${restored_null}')"
}

create_table "local" "${table}" "" "${url_definition}"
create_table "on cluster" "${table}" "${cluster}" "${url_definition}"
create_table "on cluster, format 2" "${table}" "${cluster}" "${url_definition}" --distributed_ddl_entry_format_version 2
create_table "copy" "${copy}" "" "${copy_definition}"
create_table "copy, on cluster" "${copy}" "${cluster}" "${copy_definition}"
create_table "copy, format 2" "${copy}" "${cluster}" "${copy_definition}" --distributed_ddl_entry_format_version 2
restore_table "restore" ""
restore_table "restore, on cluster" "${cluster}"
restore_to_null "restore to null"
echo "-- no table"
count_tables

# Positive control: with the grant, the same queries are allowed, so the refusals above come from the missing grant.
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON URL TO ${user}"
create_table "on cluster, granted" "${table}" "${cluster}" "${url_definition}"
create_table "copy, granted" "${copy}" "" "${copy_definition}"
restore_table "restore, granted" "${cluster}"
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON Null TO ${user}"
restore_to_null "restore to null, granted"
echo "-- tables created"
count_tables

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
