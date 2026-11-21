#!/usr/bin/env bash
# this test doesn't need 'current_database = currentDatabase()',

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A connection pool of its own: a pooled connection would skip the handshake.
ADDR="127.$((RANDOM % 250 + 2)).$((RANDOM % 250 + 2)).$((RANDOM % 250 + 2))"
query_id="${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$query_id" -q "
    SELECT count() FROM remote('127.0.0.1,$ADDR', numbers(20)) WHERE sleepEachRow(0.5) = 0
    SETTINGS max_threads = 1, max_block_size = 1, prefer_localhost_replica = 1, async_query_sending_for_remote = 1"
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT dateDiff('millisecond', initial_query_start_time_microseconds, query_start_time_microseconds) BETWEEN 0 AND 2999
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND initial_query_id = '$query_id'
        AND NOT is_initial_query AND type = 'QueryFinish';
    SELECT ProfileEvents['SuspendSendingQueryToShard'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id'
        AND is_initial_query AND type = 'QueryFinish'"
