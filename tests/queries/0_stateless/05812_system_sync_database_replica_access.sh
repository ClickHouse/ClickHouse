#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05812_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}_repl"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db} SYNC"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db}', 's1', 'r1');
CREATE USER ${user};
EOF

echo "-- without the grant"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SYSTEM SYNC DATABASE REPLICA ${db}" 2>&1 | grep -o -m1 "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SYSTEM SYNC DATABASE REPLICA ${CLICKHOUSE_DATABASE}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "-- with the grant"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM SYNC DATABASE REPLICA ON ${db}.* TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SYSTEM SYNC DATABASE REPLICA ${db}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SYSTEM SYNC DATABASE REPLICA ${CLICKHOUSE_DATABASE}" 2>&1 | grep -o -m1 "ACCESS_DENIED"
