#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the pause failpoint is global to the server and would hold the async inserts of other tests.
# Tag no-fasttest: needs failpoints.

# Two flushes of one key overlap, and the older one finishes while the newer one is still running.
# An insert with the key must then wait for its busy timeout, as another client inserts with it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=async_insert_pause_before_schedule
trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_overlap (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t_overlap_sink (x UInt8) ENGINE = Null;
    -- The flush of a row with value 1 sleeps for 3 seconds.
    CREATE MATERIALIZED VIEW t_overlap_mv TO t_overlap_sink AS SELECT sleepEachRow(3) AS x FROM t_overlap WHERE n = 1;"

# A large maximum busy timeout, so that an insert is never flushed at once because the previous one was longer than it ago.
# Other tests leave busy timeouts of up to 600000 ms in the queue shards. The huge decrease rate drops such a timeout to
# the minimum at the first insert.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 1 \
        --async_insert_busy_timeout_min_ms 500 --async_insert_busy_timeout_max_ms 100000 \
        --async_insert_busy_timeout_decrease_rate 1000000000 \
        --query_id "${CLICKHOUSE_DATABASE}_$1" --query "INSERT INTO t_overlap VALUES ($2)"
}

# Waits until the insert was pushed to the queue and has left it again, so its flush has started. It can leave the queue
# before a poll sees it there, so the push is detected by its `AsyncInsertQuery` profile event instead.
function wait_for_flush_start()
{
    local query_id="${CLICKHOUSE_DATABASE}_$1"
    for _ in {1..600}; do
        [[ $(${CLICKHOUSE_CLIENT} --query "
            SELECT (SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND ProfileEvents['AsyncInsertQuery'] = 1)
                AND (SELECT count() = 0 FROM system.asynchronous_inserts WHERE has(entries.query_id, '${query_id}'))") == 1 ]] && return
        sleep 0.05
    done
    echo "The flush of $1 did not start"
}

# The first insert can be flushed at once when the queue shard was idle for long. The second one comes right after it,
# so it waits for its deadline alone, and the key does not get batched.
insert w1 0
insert w2 0

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

# Flushed at once, and held before the pool.
insert a 0 &
pid_a=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

# A flush of the key is in progress, so this insert waits for its deadline, and is then held as well. Its flush sleeps.
insert b 1 &
wait_for_flush_start b

# Both flushes run now; the first one finishes at once, the second one sleeps.
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
wait $pid_a

# The flush of b is still in progress, so this insert must wait for its busy timeout.
insert c 0
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT query_duration_ms >= 500 FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_id = '${CLICKHOUSE_DATABASE}_c';
    SELECT count() FROM t_overlap;"
