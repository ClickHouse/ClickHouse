#!/usr/bin/env bash
# Tags: no-sanitizers
# The adaptive decision is based on the measured wall-clock costs, and the sanitizers change the relative
# cost of the mask operations and of `compareSubstrings` so much that the short-circuiting can look non-beneficial.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `compareSubstrings` is executed lazily under a selective outer `if` (about 1% of the rows) and an inner `and`.
# If it was executed eagerly, it would run on every row of the block, before the masks of its lazy ancestors
# are applied. The adaptive heuristic must cost the eager alternative over the whole block, not over the rows
# passed to the nearest lazy ancestor, otherwise it disables the useful short-circuiting of an expensive argument.

SETTINGS="short_circuit_function_evaluation = 'force_enable', max_threads = 1, max_block_size = 8192, compile_expressions = 0, compile_aggregate_expressions = 0"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_nested_selective (ok UInt8, b UInt8, s1 String, s2 String) ENGINE = Memory;
    INSERT INTO t_nested_selective
    SELECT number % 101 = 0, number % 7 != 0, repeat('a', 256), concat(repeat('a', 255), 'b')
    FROM numbers(100000)
    SETTINGS max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 0;"

QUERY="SELECT sum(if(ok, and(b, compareSubstrings(s1, s2, 0, 0, 256)), 0)) FROM t_nested_selective"

${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 0"

# The result must not change, and `compareSubstrings` must stay lazy.
# The decision is based on wall-clock measurements of a few hundred rows per round, so on a heavily loaded
# machine an unlucky run can still look non-beneficial. Retry a few times: with the wrong cost model (costing
# the eager alternative over the rows passed to the nearest lazy ancestor) the switch happens in every run.
for _ in {1..5}
do
    result=$(${CLICKHOUSE_CLIENT} --send_logs_level trace --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 1" 2> "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log")
    switches=$(grep -c "Lazy execution is not beneficial for compareSubstrings" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log")
    [ "$switches" = "0" ] && break
done
echo "$result"
echo "$switches"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_nested_selective"
