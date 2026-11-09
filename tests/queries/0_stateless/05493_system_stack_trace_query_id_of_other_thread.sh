#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.stack_trace` attributes the stack of a thread running another query to that query.

query_id="${CLICKHOUSE_DATABASE}_sleep"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT sleepEachRow(1) FROM numbers(60) SETTINGS max_block_size = 1 FORMAT Null" >/dev/null 2>&1 &

# Only the threads of the sleeping query are read, and a capture can miss a thread, so it is retried.
result=0
for _ in {1..60}
do
    result=$($CLICKHOUSE_CLIENT -q "
        SELECT count() > 0
        FROM system.stack_trace
        WHERE thread_id IN (SELECT arrayJoin(thread_ids) FROM system.processes WHERE query_id = '$query_id')
            AND query_id = '$query_id'
            AND arrayExists(x -> demangle(addressToSymbol(x)) LIKE '%FunctionSleep%', trace)
        SETTINGS allow_introspection_functions = 1")
    [ "$result" = "1" ] && break
    sleep 0.5
done
echo "$result"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait
