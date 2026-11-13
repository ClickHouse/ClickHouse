#!/usr/bin/env bash

# `CREATE TABLE ... AS url(...)` needs `TABLE ENGINE ON URL`, like `ENGINE = URL(...)`, which a filtered source grant
# does not imply. `ON CLUSTER` builds the table on the DDL workers with full access, so the grant must be required
# before the query is sent to them: with explicit columns, and with the legacy entry format, which is not normalized
# on the initiator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_$(random_str 8)"
table="table_$(random_str 8)"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.*, CLUSTER, CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT READ, WRITE ON URL('^http://localhost:11111/allowed/.*$') TO ${user};
"

# Print the header of `scenario`, run `CREATE TABLE ... AS url(...)` as the user with `on_cluster` and the extra
# client arguments, and print the error code and the missing grant, or `OK`.
function create_as_url()
{
    local scenario=$1 on_cluster=$2
    shift 2
    echo "-- ${scenario}"
    local error
    error=$($CLICKHOUSE_CLIENT --user "${user}" "$@" -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.${table} ${on_cluster} (x UInt32)
        AS url('http://localhost:11111/allowed/data.tsv', 'TSV', 'x UInt32')" 2>&1 \
        | grep -o "ACCESS_DENIED\|TABLE ENGINE ON URL" | sort -u | paste -sd ' ')
    echo "${error:-OK}"
}

create_as_url "local" ""
create_as_url "on cluster" "ON CLUSTER test_shard_localhost"
create_as_url "on cluster, format 2" "ON CLUSTER test_shard_localhost" --distributed_ddl_entry_format_version 2
echo "-- no table"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${CLICKHOUSE_DATABASE}.${table}"

# Positive control: with the grant, the same query is allowed, so the refusals above come from the missing grant.
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON URL TO ${user}"
create_as_url "on cluster, granted" "ON CLUSTER test_shard_localhost"
echo "-- table created"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${CLICKHOUSE_DATABASE}.${table}"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.${table}"
$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
