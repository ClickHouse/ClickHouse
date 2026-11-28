#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the pause failpoint is global to the server and would hold the async inserts of other tests.
# Tag no-fasttest: needs failpoints.

# An `INSERT ... SELECT` that goes to the async insert queue holds a shared lock of the destination table while it pushes
# its block. When its key is flushed at once, the flush can wait for the pool, which can be busy with a flush that waits
# behind an exclusive lock of the table. The insert must not wait for the pool while it holds the lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=async_insert_pause_before_schedule
trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_flush_lock (n UInt64) ENGINE = MergeTree ORDER BY n"

# The busy timeout of the queue shard is shared with other tests, which leave values of up to 600000 ms in it. The huge
# decrease rate drops it to the minimum, unless another insert came to the shard less than the minimum before; then the
# moderate maximum bounds the wait. The maximum is still longer than any pause between the inserts, so none of them is
# flushed at once because the previous one was longer than the maximum ago.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_select_as_async_insert 1 \
        --async_insert_use_adaptive_busy_timeout 1 --async_insert_busy_timeout_min_ms 500 \
        --async_insert_busy_timeout_max_ms 20000 --async_insert_busy_timeout_decrease_rate 1000000000 \
        --query_id "${CLICKHOUSE_DATABASE}_$1" --query "INSERT INTO t_flush_lock SELECT 1"
}

# The key is not batched once an insert waited for its deadline alone.
insert w1
insert w2

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

# Flushed at once, and held before the pool.
insert c &
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

# An atomic `POPULATE` takes an exclusive lock of its source table. The insert waits for its flush, but must not hold
# the shared lock of the table meanwhile.
${CLICKHOUSE_CLIENT} --lock_acquire_timeout 5 --materialized_views_populate_atomically 1 --query "
    CREATE MATERIALIZED VIEW t_flush_lock_mv ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM t_flush_lock" \
    && echo "CREATE MATERIALIZED VIEW is not blocked"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['AsyncInsertFlushedWithoutBatching'] FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_id = '${CLICKHOUSE_DATABASE}_c';
    SELECT count() FROM t_flush_lock;"
