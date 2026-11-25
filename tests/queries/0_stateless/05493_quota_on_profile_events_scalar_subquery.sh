#!/usr/bin/env bash
# A scalar subquery is executed while the query is analyzed, before the quotas of the query are checked.
# An exhausted quota over profile events must reject it before it reads anything, with and without the analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U="user_05493_${CLICKHOUSE_DATABASE}"
Q="quota_05493_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedRows = 1 TO ${U}"

echo "-- exhaust the quota"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sum(number) FROM numbers(2)"

for analyzer in 1 0
do
    echo "-- enable_analyzer = ${analyzer}: the scalar subquery is rejected before it reads"
    ${CLICKHOUSE_CLIENT} --user "${U}" --enable_analyzer "${analyzer}" -q "SELECT (SELECT sum(number) FROM numbers(1000)) FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedRows'] < 100 FROM system.quotas_usage WHERE quota_name = '${Q}'"
done

cleanup
