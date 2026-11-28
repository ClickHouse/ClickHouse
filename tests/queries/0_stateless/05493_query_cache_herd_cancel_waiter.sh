#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query waiting for a concurrent, identical query to compute the result for the query result cache ("thundering herd"
# avoidance) must react to the cancellation by its client while the executor is still running: the native protocol
# receives the `Cancel` packet only when the server polls the connection.

EXECUTOR_QUERY_ID="05493_executor_${CLICKHOUSE_DATABASE}"
WAITER_QUERY_ID="05493_waiter_${CLICKHOUSE_DATABASE}"

QUERY="SELECT sleepEachRow(1) FROM numbers(120) \
    SETTINGS use_query_cache = 1, \
             query_cache_min_query_duration = 0, \
             query_cache_min_query_runs = 0, \
             query_cache_herd_wait_timeout = 600, \
             query_cache_nondeterministic_function_handling = 'save', \
             max_block_size = 1, \
             max_execution_time = 600"

function query_is_running()
{
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" -ge 1 ]
}

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id "${EXECUTOR_QUERY_ID}" >/dev/null 2>&1 &
executor_pid=$!

while ! query_is_running "${EXECUTOR_QUERY_ID}"; do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id "${WAITER_QUERY_ID}" >/dev/null 2>&1 &
waiter_pid=$!

while ! query_is_running "${WAITER_QUERY_ID}"; do
    sleep 0.1
done

# The client sends the `Cancel` packet on SIGINT.
kill -INT "${waiter_pid}"
wait "${waiter_pid}"

# The waiter is gone while the executor is still running.
while query_is_running "${WAITER_QUERY_ID}"; do
    sleep 0.1
done
query_is_running "${EXECUTOR_QUERY_ID}" && echo "executor is still running"

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${EXECUTOR_QUERY_ID}' SYNC" >/dev/null
wait "${executor_pid}"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
