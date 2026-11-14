#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: Clears the global partial aggregate cache.

# `SYSTEM DROP AGGREGATE CACHE` and its aliases require the `SYSTEM DROP AGGREGATE CACHE` privilege, locally and `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05458_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO ${user}"

queries=(
    "SYSTEM DROP AGGREGATE CACHE"
    "SYSTEM CLEAR AGGREGATE CACHE"
    "SYSTEM DROP PARTIAL AGGREGATE CACHE"
    "SYSTEM CLEAR PARTIAL AGGREGATE CACHE"
    "SYSTEM DROP AGGREGATE CACHE ON CLUSTER test_shard_localhost"
)

echo "--- without the grant"
for query in "${queries[@]}"; do
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "${user}" --query "${query} -- { serverError ACCESS_DENIED }"
done

${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP AGGREGATE CACHE ON *.* TO ${user}"

echo "--- with the grant"
for query in "${queries[@]}"; do
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "${user}" --query "${query}" && echo "OK"
done

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
