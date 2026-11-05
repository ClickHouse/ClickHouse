#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# `throwIf` with one argument is compiled with errors reported to the caller. Compiled and interpreted execution must give
# the same results and fail with the same error code and message.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JIT="--compile_expressions=1 --min_count_to_compile_expression=0"
NO_JIT="--compile_expressions=0"

# Prints the result of a query, or "Code N: message" of its exception without the context added by the executor.
function outcome()
{
    local settings=$1
    local query=$2
    local result
    # shellcheck disable=SC2086
    result=$($CLICKHOUSE_CLIENT $settings --max_threads=1 --query "$query" 2>&1)
    if echo "$result" | grep -q "^Code: "; then
        echo "$result" | grep "^Code: " | sed -E 's/^Code: ([0-9]+)\..*DB::Exception: ([^:(]*[^:( ]).*/Code \1: \2/'
    else
        echo "$result" | tr '\n' ' '
        echo
    fi
}

# Runs a query with and without the JIT and prints the outcome if both agree.
function compare()
{
    local name=$1
    local query=$2
    local compiled interpreted
    compiled=$(outcome "$JIT" "$query")
    interpreted=$(outcome "$NO_JIT" "$query")
    if [ "$compiled" == "$interpreted" ]; then
        echo "$name: $compiled"
    else
        echo "$name: DIFFERENT"
        echo "  compiled: $compiled"
        echo "  interpreted: $interpreted"
    fi
}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_throw_if"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_throw_if (k UInt32, a Int32, f Float64, n Nullable(Int32)) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_throw_if VALUES (0, 1, 0, NULL), (1, 2, -0., 0), (2, 3, nan, NULL), (3, 10, 1.5, 0)"

compare "no row fails" "SELECT groupArray(throwIf(a > 100) + a) FROM jit_throw_if"
compare "a row fails" "SELECT groupArray(throwIf(a > 5) + a) FROM jit_throw_if"
compare "negative zero fails" "SELECT groupArray(throwIf(f * 1) + a) FROM jit_throw_if WHERE k = 1"
compare "NaN fails" "SELECT groupArray(throwIf(f * 1) + a) FROM jit_throw_if WHERE k = 2"
compare "zero does not fail" "SELECT groupArray(throwIf(f * 1) + a) FROM jit_throw_if WHERE k = 0"
compare "NULL rows do not fail" "SELECT groupArray(throwIf(n + 0) + a) FROM jit_throw_if"
compare "NULL rows do not fail, non-zero value" "SELECT groupArray(throwIf(n + 1) + a) FROM jit_throw_if WHERE k = 0 OR k = 2"
compare "guarded in if" "SELECT groupArray(if(a < 5, throwIf(a > 5) + a, 0)) FROM jit_throw_if"
compare "in PREWHERE" "SELECT count() FROM jit_throw_if PREWHERE throwIf(a > 5) + a > 1"

# The function ran compiled.
$CLICKHOUSE_CLIENT $JIT --log_comment "05502_compiled_$CLICKHOUSE_DATABASE" --query "SELECT sum(throwIf(a > 100) + a) FROM jit_throw_if FORMAT Null"
# With a `Nullable` argument whose value under the NULL rows is not zero.
$CLICKHOUSE_CLIENT $JIT --log_comment "05502_compiled_nullable_$CLICKHOUSE_DATABASE" --query "SELECT groupArray(throwIf(n + 1) + a) FROM jit_throw_if WHERE k = 0 OR k = 2 FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'compiled', ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05502_compiled_$CLICKHOUSE_DATABASE'
        AND event_date >= yesterday()"
$CLICKHOUSE_CLIENT --query "
    SELECT 'compiled with Nullable', ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05502_compiled_nullable_$CLICKHOUSE_DATABASE'
        AND event_date >= yesterday()"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_throw_if"
