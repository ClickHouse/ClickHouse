#!/usr/bin/env bash

# A client that waits for each insert gets its inserts flushed at once. Another client starts inserting with the same key
# between those short flushes, so their inserts never overlap. An insert of the first client must still wait for its busy
# timeout after a while, and be batched with an insert of the second one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_sequential_then_another (n UInt64) ENGINE = MergeTree ORDER BY n;
    SYSTEM STOP MERGES t_sequential_then_another;"

# The busy timeout of the queue shard is shared with other tests, which can raise it to the maximum, so the maximum is
# kept moderate. It is still longer than any pause between the inserts, so none of them is flushed at once because the
# previous one was longer than the maximum ago.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 1 \
        --async_insert_busy_timeout_min_ms 3000 --async_insert_busy_timeout_max_ms 20000 \
        --query_id "${CLICKHOUSE_DATABASE}_$1" --query "INSERT INTO t_sequential_then_another VALUES ($2)"
}

# The key is not batched once an insert waited for its deadline alone.
insert w1 0
insert w2 1

STOP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_stop"
rm -f "${STOP_FILE}"

# The first client: its inserts are flushed at once, until one waits for its busy timeout.
(
    for i in {1..40}; do
        [[ -f "${STOP_FILE}" ]] && break
        insert "a_$i" $((i + 1))
    done
) &
pid_a=$!

# An insert of the first client that waits for its busy timeout. An insert flushed at once can be seen in the queue too,
# until the deadline thread takes it, but its deadline is not in the future. `first_update` is the deadline of a batch.
waiting=0
while kill -0 $pid_a 2>/dev/null; do
    if [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.asynchronous_inserts
        WHERE database = currentDatabase() AND table = 't_sequential_then_another'
            AND arrayExists(query_id -> startsWith(query_id, '${CLICKHOUSE_DATABASE}_a_'), entries.query_id)
            AND first_update > now64(6) + INTERVAL 1 SECOND") != 0 ]]; then
        waiting=1
        break
    fi
    sleep 0.05
done
[[ $waiting == 1 ]] || echo "No insert of the first client waited for its busy timeout"

# The second client inserts while the insert of the first one waits, so they are batched. The first client stops after it.
touch "${STOP_FILE}"
insert b 100
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    -- Inserts of the first client were flushed at once before one waited.
    SELECT countIf(ProfileEvents['AsyncInsertFlushedWithoutBatching'] = 1) > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND startsWith(query_id, '${CLICKHOUSE_DATABASE}_a_');
    SELECT max(rows) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_sequential_then_another' AND active;"

rm -f "${STOP_FILE}"
