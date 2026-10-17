#!/usr/bin/env bash
# Quotas over profile events charge what the remote server reads for a distributed `INSERT`, e.g. a
# materialized view joining a table. Unlike for a distributed `SELECT`, the initiator does not count
# `SelectedRows` from the progress of the remote server, so it is taken from its profile events.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05402_${CLICKHOUSE_DATABASE}"
Q="quota_05402_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_05402"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst_05402"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_05402"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dim_05402"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dim_05402 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "INSERT INTO dim_05402 SELECT number, number * 10 FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_05402 (k UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_05402 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_05402 TO dst_05402 AS SELECT s.k AS k, d.v AS v FROM src_05402 AS s INNER JOIN dim_05402 AS d ON s.k = d.k"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.src_05402 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedRows = 1000000 TO ${U}"

# 127.0.0.2 is not a local address of the server, so the materialized view runs on the remote server only.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.src_05402) VALUES (1), (2)"
${CLICKHOUSE_CLIENT} -q "SELECT k, v FROM dst_05402 ORDER BY k"
# The view reads the 100 rows of `dim_05402`.
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedRows'] >= 100 FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
