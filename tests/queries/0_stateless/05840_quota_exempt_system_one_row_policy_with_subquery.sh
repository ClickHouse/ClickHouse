#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: a row policy on `system.one` is a server-wide side effect. While it exists, every other
# user is denied a `FROM`-less `SELECT` (`throw_on_unmatched_row_policies` is enabled in the test
# config), so concurrently running tests would fail with `ACCESS_DENIED`.

# A query that reads only `system.one` is exempt from quotas, but not when the row policy of
# `system.one` runs a subquery that reads a table: then it is charged like any other query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U="user_05840_${CLICKHOUSE_DATABASE}"
Q="quota_05840_${CLICKHOUSE_DATABASE}"
P="policy_05840_${CLICKHOUSE_DATABASE}"
T="${CLICKHOUSE_DATABASE}.t_05840"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${P} ON system.one"
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${T}"
}
# The row policy on `system.one` must never outlive the test, even if it fails early.
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${T} (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${T} SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${T} TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX queries = 1000, SelectedRows = 1000000000 TO ${U}"

usage() { ${CLICKHOUSE_CLIENT} -q "SELECT queries, profile_events['SelectedRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"; }
read -r prev_queries prev_rows <<< "$(usage)"

# Prints whether the query was charged to `queries` and to the quota over `SelectedRows`.
check()
{
    ${CLICKHOUSE_CLIENT} --user "${U}" -q "$2" > /dev/null
    read -r queries rows <<< "$(usage)"
    echo -e "$1\t$(( queries > prev_queries ))\t$(( rows > prev_rows ))"
    prev_queries=${queries}
    prev_rows=${rows}
}

check "no row policy" "SELECT dummy FROM system.one"
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY ${P} ON system.one USING dummy = 0 TO ${U}"
check "row policy" "SELECT dummy FROM system.one"
${CLICKHOUSE_CLIENT} -q "ALTER ROW POLICY ${P} ON system.one USING dummy IN (SELECT x FROM ${T})"
check "row policy with subquery" "SELECT dummy FROM system.one"
check "row policy with subquery, no FROM" "SELECT 1"
