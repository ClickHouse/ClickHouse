#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EXECUTE AS <user> <statement>` returns what `<statement>` returns when sent directly: the construction settings
# (`limit`, `offset`, `page`, `select`, `filter`, `order`, `sort`) in the statement's own SETTINGS clause and the HTTP
# `filter` parameter apply to it.

# Users are a global namespace: keep the name unique per test database.
user="u_05565_${CLICKHOUSE_DATABASE}"
user_const="u2_05565_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}, ${user_const}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (a UInt8) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_const} SETTINGS filter = '' CONST"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user_const}"

echo "-- limit, offset"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} SELECT a FROM t ORDER BY a SETTINGS limit = 1, offset = 1"

echo "-- page"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} SELECT a FROM t ORDER BY a SETTINGS limit = 1, page = 3"

echo "-- sort"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} SELECT a FROM t SETTINGS sort = '-a', limit = 1"

echo "-- select, filter, order"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} SELECT a FROM t SETTINGS select = 'a * 10', filter = 'a > 1', order = 'a DESC'"

echo "-- UNION with query-level SETTINGS"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} SELECT a FROM t UNION ALL SELECT a + 10 FROM t SETTINGS order = 'a', limit = 2"

echo "-- EXPLAIN"
${CLICKHOUSE_CLIENT} --query "EXECUTE AS ${user} EXPLAIN SELECT a FROM t SETTINGS filter = 'a = 2'" \
    | grep -q Filter && echo "filtered" || echo "not filtered"

echo "-- HTTP filter parameter"
# A GET request is readonly, and EXECUTE AS needs the IMPERSONATE privilege, so this is a POST.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&filter=a%20%3D%202" -d "EXECUTE AS ${user} SELECT a FROM t ORDER BY a"

echo "-- HTTP filter parameter, target user's filter is CONST"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&filter=a%20%3D%202" -d "EXECUTE AS ${user_const} SELECT a FROM t ORDER BY a" \
    | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
