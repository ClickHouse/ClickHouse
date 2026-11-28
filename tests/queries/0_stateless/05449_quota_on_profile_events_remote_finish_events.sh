#!/usr/bin/env bash
# The profile events a remote server increments while it finishes a query (`QueryTimeMicroseconds` and friends,
# incremented when the query is logged) are reported to the initiator and charged to its quotas as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05449_${CLICKHOUSE_DATABASE}"
Q="quota_05449_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectQueryTimeMicroseconds = 1000000000000 TO ${U}"

# 127.0.0.2 is not a local address of the server, so the query goes over the network. Both the initiator and
# the remote server run for at least half a second, so their times add up to at least a second.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sleep(0.5) FROM remote('127.0.0.2', system.one) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectQueryTimeMicroseconds'] >= 1000000 FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
