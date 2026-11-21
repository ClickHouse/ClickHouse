#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
quota="${CLICKHOUSE_TEST_UNIQUE_NAME}_quota"
key="${CLICKHOUSE_TEST_UNIQUE_NAME}_tenant"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}; DROP QUOTA IF EXISTS ${quota}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}; GRANT SELECT, SHOW QUOTAS ON *.* TO ${user}"

check_snapshot()
{
    local expected_key="$1"
    local supplied_key="$2"
    # One native connection: the ordinary query meters the quota, then introspection compares
    # the registry snapshot with the usage bucket actually selected by the native handshake.
    ${CLICKHOUSE_CLIENT} --user="${user}" --quota_key="${supplied_key}" --multiquery --query "
        SELECT number FROM numbers(1) FORMAT Null;
        SELECT throwIf(count() != 1 OR countIf(has(s.quotas, (q.quota_name, q.quota_key)) AND q.quota_key = '${expected_key}') != 1)
        FROM system.sessions AS s CROSS JOIN system.quota_usage AS q
        WHERE s.current_query_id = queryID() AND q.quota_name = '${quota}' AND q.quota_key = '${expected_key}'
        FORMAT Null;"
}

${CLICKHOUSE_CLIENT} --query "CREATE QUOTA ${quota} KEYED BY client_key FOR INTERVAL 1 hour MAX queries = 100 TO ${user}"
check_snapshot "${key}" "${key}"
echo 'native client_key snapshot OK'

# Login introspection must not bypass enforcement for an ordinary query without a key.
if output=$(${CLICKHOUSE_CLIENT} --user="${user}" --quota_key='' --query 'SELECT number FROM numbers(1)' 2>&1); then
    echo 'Query without a required quota key unexpectedly succeeded' >&2
    exit 1
fi
grep -q 'QUOTA_REQUIRES_CLIENT_KEY' <<< "${output}"
echo 'missing client_key remains rejected OK'

${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${quota}; CREATE QUOTA ${quota} KEYED BY client_key, user_name FOR INTERVAL 1 hour MAX queries = 100 TO ${user}"
check_snapshot "${key}" "${key}"
check_snapshot "${user}" ''
echo 'native client_key or user_name snapshots OK'
