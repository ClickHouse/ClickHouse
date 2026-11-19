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

# Count the expression 10 times without compiling it.
for _ in {1..10}; do echo "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null;"; done | $CLICKHOUSE_CLIENT

# Count 12000 other expressions (4 queries x 3000 columns): more than the counter keeps, so it is cleared.
for q in 0 1 2 3; do
    cols=$(seq $((L + q * 3000 + 1)) $((L + q * 3000 + 3000)) | sed 's/.*/(number + &) * 2/' | paste -sd, -)
    $CLICKHOUSE_CLIENT -q "SELECT $cols FROM numbers(1) SETTINGS $NO_COMPILE FORMAT Null"
done

# Counted 10 times before the clear, so without the clear it would be compiled here.
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_check" -q "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 10 FORMAT Null"
# Positive control: the expression is compilable on this build.
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_control" -q "SELECT (number + $L) * 2 FROM numbers(1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0 FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(query_id, currentDatabase() || '_', ''), ProfileEvents['CompileFunction'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('${CLICKHOUSE_DATABASE}_check', '${CLICKHOUSE_DATABASE}_control')
    ORDER BY query_id"
