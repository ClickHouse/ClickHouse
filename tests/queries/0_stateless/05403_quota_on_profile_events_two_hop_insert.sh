#!/usr/bin/env bash
# Quotas over profile events charge what the servers further down do for a distributed `INSERT` that
# crosses more than one hop: the intermediate server forwards the reports it receives to the initiator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05403_${CLICKHOUSE_DATABASE}"
Q="quota_05403_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dist_05403"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS leaf_05403"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE leaf_05403 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_05403 (a UInt32) ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, leaf_05403)"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.dist_05403 TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.leaf_05403 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX MergeTreeDataWriterRows = 1 TO ${U}"

# The initiator sends the rows to 127.0.0.2, whose `Distributed` table synchronously sends them over the
# network once more (`prefer_localhost_replica = 0`), to the server that writes them to `leaf_05403`.
SETTINGS="--async_insert 0 --distributed_foreground_insert 1 --prefer_localhost_replica 0"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} --user "${U}" ${SETTINGS} -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.dist_05403) VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM leaf_05403"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['MergeTreeDataWriterRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} --user "${U}" ${SETTINGS} -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.dist_05403) VALUES (4)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM leaf_05403"

cleanup
