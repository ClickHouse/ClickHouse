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

# A large maximum busy timeout, so that an insert is never flushed at once because the previous one was longer than it ago.
# Other tests leave busy timeouts of up to 600000 ms in the queue shards. The huge decrease rate drops such a timeout to
# the minimum at the first insert.
function insert()
{
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 1 \
        --async_insert_busy_timeout_min_ms 3000 --async_insert_busy_timeout_max_ms 100000 \
        --async_insert_busy_timeout_decrease_rate 1000000000 \
        --query_id "${CLICKHOUSE_DATABASE}_$1" --query "INSERT INTO t_sequential_then_another VALUES ($2)"
}

# The first insert can be flushed at once when the queue shard was idle for long. The second one comes right after it,
# so it waits for its deadline alone, and the key does not get batched.
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

# An insert that waits in the queue. Inserts flushed at once never show up there.
waiting=0
while kill -0 $pid_a 2>/dev/null; do
    if [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.asynchronous_inserts
        WHERE database = currentDatabase() AND table = 't_sequential_then_another'") != 0 ]]; then
        waiting=1
        break
    fi
    sleep 0.05
done
[[ $waiting == 1 ]] || echo "No insert of the first client waited for its busy timeout"

# The second client inserts while the insert of the first one waits, so they are batched.
insert b 100
touch "${STOP_FILE}"
wait

${CLICKHOUSE_CLIENT} --query "
    SELECT max(rows) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_sequential_then_another' AND active;"

rm -f "${STOP_FILE}"
