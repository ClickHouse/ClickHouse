#!/usr/bin/env bash

# A client that waits for each insert gets its inserts flushed at once, without updating the time of the last insert used
# for the busy timeout. Such a stream must not make the queue shard look idle once it lasts longer than the maximum busy
# timeout: then an insert would be flushed by the inserting thread as the first one after a long pause.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_fast_stream (n UInt64) ENGINE = MergeTree ORDER BY n"

# The huge decrease rate drops a busy timeout left in the queue shard by other tests to the minimum.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 1 \
        --async_insert_busy_timeout_min_ms 100 --async_insert_busy_timeout_max_ms 2000 \
        --async_insert_busy_timeout_decrease_rate 1000000000 \
        --query_id "${CLICKHOUSE_DATABASE}_$1" --query "INSERT INTO t_fast_stream VALUES ($1)"
}

# The first insert waits for its busy timeout alone, or, if the queue shard was idle for long, is flushed at once and the
# second one waits. The next 16 are flushed without batching, then one waits again. That one is flushed more than the
# maximum after the first, so the queue shard has seen no insert for the maximum by the times of its flushes alone. The
# inserts after it are flushed without batching too, although they continue for longer than the maximum.
for i in {0..29}; do
    insert "$i"
    sleep 0.15
done

${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT countIf(ProfileEvents['AsyncInsertFlushedWithoutBatching'] = 0), count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_kind = 'Insert' AND query_id LIKE '${CLICKHOUSE_DATABASE}\\_%'
        AND query_id NOT IN ('${CLICKHOUSE_DATABASE}_0', '${CLICKHOUSE_DATABASE}_1');
    DROP TABLE t_fast_stream;"
