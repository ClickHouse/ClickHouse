#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

# An entry format before NORMALIZE_CREATE_ON_INITIATOR_VERSION ships CREATE TABLE AS to the worker, which copies the
# columns that an `Alias` source reports from its target with no user, whatever the engine. So the initiator requires
# SHOW COLUMNS on the target, as a local CREATE TABLE AS does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${db}.hidden (id UInt64, secret String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${db}.alias_src ENGINE = Alias('${db}', 'hidden');

    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT CLUSTER ON *.* TO ${user};
    GRANT TABLE ENGINE ON Memory TO ${user};
    GRANT SHOW COLUMNS ON ${db}.alias_src TO ${user};
"

function try_copy()
{
    local version=$1
    echo "-- entry format ${version}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version "${version}" \
        -q "CREATE TABLE ${db}.copy_${version} ON CLUSTER test_shard_localhost AS ${db}.alias_src ENGINE = Memory" 2>&1 \
        | grep -oE "Not enough privileges to describe metadata exposed by" | head -n 1
}

try_copy 1
try_copy 2

${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON ${db}.hidden TO ${user}"
try_copy 2
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.columns WHERE database = '${db}' AND table = 'copy_2' ORDER BY position"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
