#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Fast test build has no embedded compiler.
# no-msan: the MSan build disables the embedded compiler as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The counter of `min_count_to_compile_expression` is bounded: once full, a new expression clears it.

# Literals unique to this run, so that concurrent runs do not count the same expressions.
L=$($CLICKHOUSE_CLIENT -q "SELECT 1000000000000 + rand64() % 1000000000000")
NO_COMPILE="compile_expressions = 1, min_count_to_compile_expression = 1000000000"

{
    # Count the expression 10 times without compiling it.
    for _ in {1..10}; do echo "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"; done

    # Count 10000 other expressions, as many as the counter keeps, so it is cleared.
    # Each `x * <literal>` is a separate expression; sharing `x` keeps it to one function per expression.
    for q in 0 1; do
        cols=$(seq $((L + q * 5000 + 1)) $((L + q * 5000 + 5000)) | sed 's/.*/x * &/' | paste -sd, -)
        echo "WITH number + 1 AS x SELECT $cols FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"
    done

    # Counted 10 times before the clear, so without the clear it would be compiled here.
    echo "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 10, log_comment = 'check' FORMAT Null;"
    # Positive control: the expression is compilable on this build.
    echo "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'control' FORMAT Null;"

    echo "SYSTEM FLUSH LOGS query_log;"
    echo "SELECT log_comment, ProfileEvents['CompileFunction'] > 0 FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('check', 'control')
        ORDER BY log_comment;"
} | $CLICKHOUSE_CLIENT
