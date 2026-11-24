#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires an `ORC`-enabled build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

user="orc_cache_user_05873_${CLICKHOUSE_DATABASE}"
cluster="test_shard_localhost"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
trap '${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"' EXIT
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO ${user}"

run() { ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1"; }
check_denied() {
    local output
    if output=$(run "$1" 2>&1); then
        echo "unexpectedly allowed"
        exit 1
    fi
    if ! grep -qF 'SYSTEM DROP ORC_METADATA_CACHE' <<< "$output"; then
        echo "$output"
        exit 1
    fi
    echo "denied"
}

check_denied "SYSTEM DROP ORC METADATA CACHE"
check_denied "SYSTEM DROP ORC METADATA CACHE ON CLUSTER ${cluster}"

${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP ORC METADATA CACHE ON *.* TO ${user}"
run "SYSTEM DROP ORC METADATA CACHE" || exit 1
echo "allowed"
run "SYSTEM DROP ORC METADATA CACHE ON CLUSTER ${cluster}" || exit 1
echo "allowed"
