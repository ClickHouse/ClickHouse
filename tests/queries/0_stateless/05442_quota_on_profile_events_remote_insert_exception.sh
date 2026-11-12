#!/usr/bin/env bash
# Quotas over profile events account the work of the remote servers for a distributed `INSERT`
# that fails on the remote server after it has received all the data: the remote server sends
# its final `ProfileEvents` report before the `Exception`, the same as on the success path.
# The constraint check runs after the squashing of the small inserted block, i.e. once the
# end-of-data terminator has been received, so `FailedInsertQuery` is counted by both the
# initiator and the remote server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05442_${CLICKHOUSE_DATABASE}"
Q="quota_05442_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05442"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05442 (a UInt32, CONSTRAINT c CHECK a < 10) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.t_05442 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX FailedInsertQuery = 100 TO ${U}"

# 127.0.0.2 is not a local address of the server, so the data goes over the network and the
# constraint is checked by the remote server only.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.t_05442) VALUES (1), (100)" 2>&1 | grep -o -m1 "VIOLATED_CONSTRAINT"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05442"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedInsertQuery'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
