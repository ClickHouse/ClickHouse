#!/usr/bin/env bash
# Tags: no-sanitizers
# The adaptive decision is based on the measured wall-clock costs, and the sanitizers change the relative
# cost of the mask operations and of `compareSubstrings` so much that the decisions become unpredictable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lazily executed argument is switched to the eager execution if the short-circuiting does not pay off, and
# it is switched back if its per-row cost grows significantly compared to the baseline of the previous rounds.
#
# The first six blocks of 8192 rows (two rounds) make `compareSubstrings` cheap: the strings consist of a single
# byte, and the outer `if` passes half of the rows, so filtering them out costs more than executing it on every
# row, and the lazy execution is switched off after these two rounds. The last block of 65536 rows makes its
# per-row cost grow many times. It is a round of its own, in which `compareSubstrings` is executed eagerly on
# all 65536 rows, which is much more than the 24576 rows it was executed on in the two lazy rounds. If the
# baseline included the current round, it would be dominated by the current round, and the cost growth would
# never be noticed: `h > 1.5 * (24576 * c + 65536 * h) / 90112` does not hold for any costs `c` and `h`.

SETTINGS="short_circuit_function_evaluation = 'force_enable', max_threads = 1, max_block_size = 8192, compile_expressions = 0, compile_aggregate_expressions = 0, enable_parallel_replicas = 0"

# `Memory` keeps the inserted blocks as they are, and a single-threaded insert keeps them in order.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_reenable (ok UInt8, s1 String, s2 String) ENGINE = Memory;

    INSERT INTO t_reenable
    SELECT number % 2 = 0, 'a', 'a'
    FROM numbers(6 * 8192)
    SETTINGS max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1;

    INSERT INTO t_reenable
    SELECT number % 100 = 0, repeat('a', 256), repeat('a', 256)
    FROM numbers(65536)
    SETTINGS max_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1;"

QUERY="SELECT sum(if(ok, compareSubstrings(s1, s2, 0, 0, 256), 1)) FROM t_reenable"

${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 0"

# The decision is based on wall-clock measurements, so on a heavily loaded machine an unlucky run may not
# switch to the eager execution in the first place, or switch too late. Retry a few times: with the current
# round included in the baseline, the lazy execution is never enabled again in any of the runs.
for _ in {1..5}
do
    result=$(${CLICKHOUSE_CLIENT} --send_logs_level trace --query "$QUERY SETTINGS $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 1" 2> "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log")
    reenabled=$(grep -c "Enable lazy execution again: compareSubstrings" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log")
    [ "$reenabled" != "0" ] && break
done
echo "$result"
[ "$reenabled" != "0" ] && echo "reenabled" || echo "not reenabled"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_reenable"
