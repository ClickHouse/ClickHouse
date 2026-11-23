#!/usr/bin/env bash
# Tags: distributed

# While a retry of a distributed query is possible (`distributed_query_retries`), the progress of the remote
# server is deferred on the initiator. If the query is cancelled before the first block of the result, that
# progress must still get into the counters of the query: `read_rows` in `system.query_log` must not be zero.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="${CLICKHOUSE_DATABASE}_distributed_query_retries_cancel_${RANDOM}"

# An aggregation returns its only block at the end, so the query can be retried for its whole duration.
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT sum(sleepEachRow(0.1)) FROM remote('127.0.0.2', numbers(600))
    SETTINGS max_block_size = 1, max_threads = 1, distributed_query_retries = 1" > /dev/null 2>&1 &

# Wait until the remote server has reported some progress to the initiator: it sends the progress
# every `interactive_delay` (100 ms), and here it has been reading for a few seconds.
while true
do
    state=$($CLICKHOUSE_CLIENT --query "
        SELECT if(count() = 0, 'absent', if(max(read_rows) >= 30, 'ready', 'waiting'))
        FROM system.processes WHERE initial_query_id = '$query_id' AND NOT is_initial_query")
    [ "$state" == "ready" ] && break
    # The query has finished or failed: the check below reports it.
    [ "$state" == "absent" ] && [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" == "0" ] && [ -z "$(jobs -r)" ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT type, read_rows > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'"
