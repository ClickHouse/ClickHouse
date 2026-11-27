#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Fast test build has no embedded compiler.
# no-msan: the MSan build disables the embedded compiler as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The counter of `min_count_to_compile_expression` is bounded: when full, expressions seen once are evicted first.
# clickhouse-local has its own counter, so concurrent tests do not change it.

NO_COMPILE="compile_expressions = 1, min_count_to_compile_expression = 1000000000"
COMPILED="value FROM system.events WHERE event = 'CompileFunction' SETTINGS system_events_show_zero_values = 1"

{
    echo "SELECT (number + 3) * 2 FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"
    for _ in {1..10}; do echo "SELECT (number + 4) * 2 FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"; done

    # 10000 other expressions seen once, as many as the counter keeps.
    # Each `x * <literal>` is a separate expression; sharing `x` keeps it to one function per expression.
    for q in 0 1; do
        cols=$(seq $((q * 5000 + 1)) $((q * 5000 + 5000)) | sed 's/.*/x * &/' | paste -sd, -)
        echo "WITH number + 1 AS x SELECT $cols FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"
    done

    echo "SELECT (number + 3) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 1 FORMAT Null;"
    echo "SELECT 'seen once', $COMPILED;"
    echo "SELECT (number + 4) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 10 FORMAT Null;"
    echo "SELECT 'seen 10 times', $COMPILED;"
} | $CLICKHOUSE_LOCAL
