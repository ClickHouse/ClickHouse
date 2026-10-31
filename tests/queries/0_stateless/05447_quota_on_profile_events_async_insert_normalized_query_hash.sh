#!/usr/bin/env bash
# The deferred flush of an asynchronous insert is charged to the same bucket of a quota keyed by `normalized_query_hash`
# as the one the insert was checked against on submission, although the flush runs a rewritten statement of its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05447_${CLICKHOUSE_DATABASE}"
Q="quota_05447_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05447"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05447 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05447 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} KEYED BY normalized_query_hash FOR INTERVAL 100 year MAX InsertedRows = 1 TO ${U}"

# With `wait_for_async_insert = 1` the insert returns after the flush, which is accounted before
# the waiting clients are notified.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t_05447 VALUES (1), (2)"
# The bucket of this query is exhausted by the flush above, so the same insert is rejected on submission.
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t_05447 VALUES (1), (2)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05447"
# A single bucket, the one of the submitted query, is charged.
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(profile_events['InsertedRows']) FROM system.quotas_usage WHERE quota_name = '${Q}' AND profile_events['InsertedRows'] > 0"

cleanup
