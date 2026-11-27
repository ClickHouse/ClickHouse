#!/usr/bin/env bash
# A query that fails to parse over HTTP is not registered in the process list, but the thread group of the
# request has accumulated the time spent parsing it, and the quotas over profile events are charged with it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U="user_05494_${CLICKHOUSE_DATABASE}"
Q="quota_05494_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX QueryParseMicroseconds = 1, FailedQuery = 1000 TO ${U}"

# A long list of expressions ending in an incomplete `FROM`: parsing it takes a while and then fails.
malformed_query="SELECT $(seq 1 1000 | sed 's/.*/number + &/' | paste -sd,) FROM"

echo "-- malformed queries fail to parse"
for _ in 1 2 3
do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U}" --data-binary "${malformed_query}" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
done

echo "-- the parsing time is charged"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryParseMicroseconds'] > 1, profile_events['FailedQuery'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

echo "-- the following query is rejected"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${U}" --data-binary "SELECT count() FROM numbers(1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

cleanup
