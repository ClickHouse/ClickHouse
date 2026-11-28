#!/usr/bin/env bash
# The flush of asynchronous inserts forced by `SYSTEM FLUSH ASYNC INSERT QUEUE` for a table runs nested in that query,
# so its profile events add up to the counters of the query as well. Having been charged to the user who submitted
# the inserts, they are not charged once more with the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05448_${CLICKHOUSE_DATABASE}"
Q="quota_05448_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05448"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05448 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05448 TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM FLUSH ASYNC INSERT QUEUE ON *.* TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX InsertedRows = 1000, AsyncInsertFlush = 1000 TO ${U}"

# The insert stays in the queue until the flush below.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 1 --wait_for_async_insert 0 --async_insert_busy_timeout_min_ms 600000 --async_insert_busy_timeout_max_ms 600000 --async_insert_use_adaptive_busy_timeout 0 \
    -q "INSERT INTO t_05448 VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --user "${U}" -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t_05448"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05448"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['InsertedRows'], profile_events['AsyncInsertFlush'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

cleanup
