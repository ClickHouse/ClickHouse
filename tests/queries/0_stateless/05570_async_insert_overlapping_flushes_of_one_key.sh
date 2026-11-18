#!/usr/bin/env bash

# Two flushes of one key overlap, and the older one finishes while the newer one is still running.
# An insert with the key must then wait for its busy timeout, as another client inserts with it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_overlap (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t_overlap_sink (x UInt8) ENGINE = Null;
    -- The flush of a row with value 2 sleeps for 6 seconds, of a row with value 1 for 9 seconds.
    CREATE MATERIALIZED VIEW t_overlap_mv_1 TO t_overlap_sink AS SELECT sleepEachRow(3) AS x FROM t_overlap WHERE n IN (1, 2);
    CREATE MATERIALIZED VIEW t_overlap_mv_2 TO t_overlap_sink AS SELECT sleepEachRow(3) AS x FROM t_overlap WHERE n IN (1, 2);
    CREATE MATERIALIZED VIEW t_overlap_mv_3 TO t_overlap_sink AS SELECT sleepEachRow(3) AS x FROM t_overlap WHERE n = 1;"

# The busy timeout of the queue shard is shared with other tests, which can raise it to the maximum, so the maximum is
# kept moderate. It is still longer than any pause between the inserts, so none of them is flushed at once because the
# previous one was longer than the maximum ago. Whether an insert is flushed at once without batching is checked with
# its profile event, not its duration.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 1 \
        --async_insert_busy_timeout_min_ms 500 --async_insert_busy_timeout_max_ms 20000 \
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

# The key is not batched once an insert waited for its deadline alone.
insert w1 0
insert w2 0

# Flushed at once. Its flush sleeps for 6 seconds.
insert a 2 &
pid_a=$!
wait_for_flush_start a

# A flush of the key is in progress, so this insert waits for its deadline. Its flush sleeps for 9 seconds.
insert b 1 &
wait_for_flush_start b

# The flush of a finishes first.
wait $pid_a

# The flush of b is still in progress, so this insert must not be flushed at once without batching.
insert c 0
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['AsyncInsertFlushedWithoutBatching']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query_id IN ('${CLICKHOUSE_DATABASE}_a', '${CLICKHOUSE_DATABASE}_b', '${CLICKHOUSE_DATABASE}_c')
    ORDER BY query_id;
    SELECT count() FROM t_overlap;"
