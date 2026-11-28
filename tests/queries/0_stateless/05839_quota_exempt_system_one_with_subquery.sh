#!/usr/bin/env bash
# A query that reads only `system.one` is exempt from quotas, but not when it also runs a subquery,
# or a `UNION` branch, that reads a table: then it is charged like any other query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05839_${CLICKHOUSE_DATABASE}"
Q="quota_05839_${CLICKHOUSE_DATABASE}"
T="${CLICKHOUSE_DATABASE}.t_05839"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${T}"
}
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

check "only system.one" "SELECT dummy FROM system.one"
check "UNION of exempt branches" "SELECT 1 UNION ALL SELECT 2"
check "IN" "SELECT count() FROM system.one WHERE dummy IN (SELECT x FROM ${T})"
check "GLOBAL IN" "SELECT count() FROM system.one WHERE dummy GLOBAL IN (SELECT x FROM ${T})"
check "no FROM" "SELECT 1 WHERE 1 IN (SELECT x FROM ${T})"
check "IN table" "SELECT count() FROM system.one WHERE dummy IN ${T}"
check "scalar subquery" "SELECT (SELECT sum(x) FROM ${T})"
check "correlated subquery" "SELECT (SELECT count() FROM ${T} WHERE x = dummy) FROM system.one"
check "UNION" "SELECT dummy FROM system.one UNION ALL SELECT x FROM ${T}"
check "additional_table_filters" "SELECT dummy FROM system.one SETTINGS additional_table_filters = {'system.one': 'dummy IN (SELECT x FROM ${T})'}"
check "additional_result_filter" "SELECT dummy FROM system.one SETTINGS additional_result_filter = 'dummy IN (SELECT x FROM ${T})'"

echo "-- exhausted quota"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${Q}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX queries = 1 TO ${U}"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT count() FROM numbers(1)"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT count() FROM system.one WHERE dummy IN (SELECT x FROM ${T})" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
# Reading only `system.one` stays possible with an exhausted quota.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT dummy FROM system.one"

cleanup
