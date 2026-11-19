#!/usr/bin/env bash
# A queued asynchronous insert whose user is dropped before the flush is logged as a failed flush,
# in `system.asynchronous_insert_log` and `system.query_log`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Users are server-global entities, so scope the name to this test's database.
U="user_${CLICKHOUSE_DATABASE}"
QUERY_ID="${CLICKHOUSE_DATABASE}_insert"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t TO ${U}"

# The insert stays in the queue until the flush below, which runs after the user is dropped.
${CLICKHOUSE_CLIENT} --user "${U}" --query_id "${QUERY_ID}" --async_insert 1 --wait_for_async_insert 0 \
    --async_insert_busy_timeout_min_ms 600000 --async_insert_busy_timeout_max_ms 600000 --async_insert_use_adaptive_busy_timeout 0 \
    -q "INSERT INTO t VALUES (1)"
${CLICKHOUSE_CLIENT} -q "DROP USER ${U}"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log, asynchronous_insert_log"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"
${CLICKHOUSE_CLIENT} -q "SELECT status, exception LIKE '%ACCESS_ENTITY_NOT_FOUND%' FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '${QUERY_ID}'"
# The flush does not run in this test's database, so its row is found by `flush_query_id`, not by `current_database = currentDatabase()`.
${CLICKHOUSE_CLIENT} -q "
    SELECT type, exception_code, user = '${U}'
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND query_id = (SELECT flush_query_id FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '${QUERY_ID}')"
