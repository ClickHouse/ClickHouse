#!/usr/bin/env bash
# Quotas over profile events of a distributed `INSERT`: the rows written by a materialized view on the remote server
# are charged in addition to the inserted rows, which are charged once, and the rows that the view reads through
# `remote` on the remote server are charged once as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05409_${CLICKHOUSE_DATABASE}"
Q="quota_05409_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_remote_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst_remote_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_remote_05409"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dim_05409"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, REMOTE ON *.* TO ${U}"

# 127.0.0.2 is not a local address of the server, so the materialized views run on the remote server only.
echo "-- the rows written by a materialized view on the remote server are charged"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_05409 (k UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_05409 (k UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_05409 TO dst_05409 AS SELECT k FROM src_05409"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.src_05409 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX InsertedRows = 1000000 TO ${U}"
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.src_05409) VALUES (1), (2)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_05409"
# 2 rows into `src_05409` and 2 rows into `dst_05409`.
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['InsertedRows'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${Q}"

echo "-- the rows read through remote by a materialized view on the remote server are charged once"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dim_05409 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "INSERT INTO dim_05409 SELECT number, number * 10 FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_remote_05409 (k UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_remote_05409 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_remote_05409 TO dst_remote_05409 AS SELECT s.k AS k, d.v AS v FROM src_remote_05409 AS s INNER JOIN remote('127.0.0.3', ${CLICKHOUSE_DATABASE}.dim_05409) AS d ON s.k = d.k"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.src_remote_05409 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedRows = 1000000, SelectedBytes = 1000000000 TO ${U}"
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 0 -q "INSERT INTO FUNCTION remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.src_remote_05409) VALUES (1), (2)"
${CLICKHOUSE_CLIENT} -q "SELECT k, v FROM dst_remote_05409 ORDER BY k"
# The view reads the 100 rows of `dim_05409` once, and the 2 inserted rows; nothing is counted twice. The exact number
# of bytes depends on the settings, but the 800 bytes of `dim_05409` are charged once.
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedRows'], profile_events['SelectedBytes'] BETWEEN 800 AND 1500 FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
