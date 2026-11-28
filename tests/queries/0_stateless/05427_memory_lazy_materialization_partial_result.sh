#!/usr/bin/env bash
# With `partial_result_on_first_cancel`, the first cancel stops the main sources, but the rows already read
# are still sorted, and the deferred columns of the selected rows must still be read by the lazy branch
# of lazy materialization of the `Memory` table, which is created after the cancellation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_memory_lazy_partial;
    CREATE TABLE t_memory_lazy_partial (k UInt64, payload String) ENGINE = Memory;
    INSERT INTO t_memory_lazy_partial SELECT number, toString(number) FROM numbers(1000000)
    SETTINGS max_threads = 1, max_block_size = 1000, max_insert_threads = 1;"

SETTINGS="max_threads = 1, max_block_size = 1000, use_top_k_dynamic_filtering = 0,
          query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
          query_plan_execute_functions_after_sorting = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"

QUERY="SELECT payload FROM t_memory_lazy_partial ORDER BY k + sleepEachRow(0.0001) LIMIT 5"

# The test is meaningful only if `payload` is deferred.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM (EXPLAIN actions = 1 $QUERY SETTINGS $SETTINGS)
    WHERE explain LIKE '%Lazily read columns: payload%'"

QUERY_ID="${CLICKHOUSE_DATABASE}_memory_lazy_partial"
CLIENT_OUT="${CLICKHOUSE_TMP}/memory_lazy_partial.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/memory_lazy_partial.err"

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" -q "
    $QUERY SETTINGS $SETTINGS, partial_result_on_first_cancel = 1, interactive_delay = 1000" > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
client_pid=$!

i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND read_rows > 0")" -lt 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Query did not start in time" >&2
        kill -9 $client_pid 2>/dev/null
        exit 1
    fi
done

kill -INT $client_pid
wait $client_pid

cat "$CLIENT_OUT"
if grep -q -F "Exception" "$CLIENT_ERR"; then
    cat "$CLIENT_ERR"
fi

rm -f "$CLIENT_OUT" "$CLIENT_ERR"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_lazy_partial"
