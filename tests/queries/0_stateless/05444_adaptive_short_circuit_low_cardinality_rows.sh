#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The default implementation for `LowCardinality` executes a function on the dictionary instead of the rows,
# but the elapsed time of the whole call is extrapolated to the other rows. The profile must record the rows
# of the call, not the dictionary size: otherwise a small dictionary inflates the estimated cost of the eager
# execution by orders of magnitude, and a cheap `LowCardinality` argument is never switched to eager execution.
# `AdaptiveShortCircuitEagerExecutions` counts the actions which the static short-circuit schedule would have
# executed lazily and the heuristic decided to execute eagerly instead.

SETTINGS="short_circuit_function_evaluation = 'force_enable', max_threads = 1, max_block_size = 8192, compile_expressions = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE lc (ok UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO lc SELECT number % 100 != 0, if(number % 2, 'a', 'b') FROM numbers(2000000);"

QUERY="SELECT sum(if(ok, compareSubstrings(s, 'z', 0, 0, 1), 0)) FROM lc"
static_result=$(${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 0")

# The decision is based on wall-clock measurements, so on a heavily loaded machine an unlucky run can keep the
# argument lazy. Retry a few times: with the dictionary size as the profiled rows the switch never happens.
results_match=1
switched=0
for attempt in {1..5}
do
    query_id="${CLICKHOUSE_DATABASE}_lc_${attempt}"
    adaptive_result=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 1")
    [ "$adaptive_result" = "$static_result" ] || results_match=0

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    switched=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['AdaptiveShortCircuitEagerExecutions'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'")
    [ "$switched" = "1" ] && break
done
echo "$static_result"
echo -e "$results_match\t$switched"
