#!/usr/bin/env bash
# The remote servers of a distributed query report their profile events to the initiator, which
# charges them to quotas over profile events, even when `send_profile_events` is turned off on the
# remote server: by a constraint of the user under which the remote server runs the query (to which
# the settings of a secondary query are clamped), or by the `SETTINGS` clause of a query that the
# remote server runs as is (`parallel_distributed_insert_select = 2`). The reports are not lost
# either when such a query fails on the remote server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05450_${CLICKHOUSE_DATABASE}"
R="remote_user_05450_${CLICKHOUSE_DATABASE}"
Q="quota_05450_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}, ${R}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_05450"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst_05450"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst_constraint_05450"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${R} SETTINGS send_profile_events = 0 CONST"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_05450 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO src_05450 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE src_05450 FINAL"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_05450 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_constraint_05450 (a UInt32, CONSTRAINT c CHECK a >= 100) ENGINE = MergeTree ORDER BY a"

for user in "${U}" "${R}"
do
    ${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_05450 TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.dst_05450 TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.dst_constraint_05450 TO ${user}"
done

# The quota is assigned only to the initiating user, so only the reports of the remote server charge it.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedParts = 1000, MergeTreeDataWriterRows = 1000 TO ${U}"

usage()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedParts'], profile_events['MergeTreeDataWriterRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
}

# 127.0.0.2 is not a local address of the server, so the queries go over the network and the tables
# are read and written by the remote server only, under the remote user.
REMOTE_SRC="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.src_05450, '${R}', '')"
REMOTE_DST="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.dst_05450, '${R}', '')"
REMOTE_DST_CONSTRAINT="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.dst_constraint_05450, '${R}', '')"

echo "constraint of the remote user"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT sum(a) FROM ${REMOTE_SRC}"
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION ${REMOTE_DST} VALUES (1), (2), (3)"
usage

echo "SETTINGS clause of an INSERT SELECT executed on the remote server"
${CLICKHOUSE_CLIENT} --user "${U}" -q "INSERT INTO FUNCTION ${REMOTE_DST} SELECT * FROM ${REMOTE_SRC}
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, send_profile_events = 0"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_05450"
usage

echo "failed INSERT SELECT executed on the remote server"
${CLICKHOUSE_CLIENT} --user "${U}" -q "INSERT INTO FUNCTION ${REMOTE_DST_CONSTRAINT} SELECT * FROM ${REMOTE_SRC}
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0" 2>&1 | grep -o -m1 "VIOLATED_CONSTRAINT"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_constraint_05450"
usage

cleanup
