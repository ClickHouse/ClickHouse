#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# `if`, `multiIf`, `and`, `or` with arguments that the interpreter evaluates lazily (short-circuit) are compiled: the lazy
# arguments are computed on every row, and their errors count only on the rows the interpreter evaluates them on.
# Every query runs compiled and interpreted, in each short-circuit mode, and must give the same result or the same error.

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

# Runs a query with and without the JIT in every short-circuit mode, and prints the outcome if they agree.
function compare()
{
    local name=$1
    local query=$2
    for mode in enable force_enable disable; do
        local compiled interpreted
        compiled=$(outcome "$JIT --short_circuit_function_evaluation=$mode" "$query")
        interpreted=$(outcome "$NO_JIT --short_circuit_function_evaluation=$mode" "$query")
        if [ "$compiled" == "$interpreted" ]; then
            echo "$name, $mode: $compiled"
        else
            echo "$name, $mode: DIFFERENT"
            echo "  compiled: $compiled"
            echo "  interpreted: $interpreted"
        fi
    done
}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_short_circuit"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_short_circuit (k UInt32, a Int32, b Int32, c Nullable(UInt8), n Nullable(Int32)) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_short_circuit VALUES
    (0, 10, 3, 1, 1), (1, -2147483648, -1, 0, NULL), (2, 7, 0, NULL, 0), (3, 9, 2, 1, 5), (4, 5, 0, 0, 0)"

compare "if guards intDiv" "SELECT groupArray(if(b != 0, intDiv(a, b), 0) + 1) FROM jit_short_circuit WHERE k != 1"
compare "if else branch guarded" "SELECT groupArray(if(b = 0, -1, intDiv(a, b)) + 1) FROM jit_short_circuit WHERE k != 1"
compare "if with Nullable condition" "SELECT groupArray(if(c, intDiv(a, b + 1), modulo(a, b)) + 1) FROM jit_short_circuit WHERE k != 1"
compare "if, minimal by minus one guarded" "SELECT groupArray(if(b != -1 AND b != 0, intDiv(a, b), 0) + 1) FROM jit_short_circuit"
compare "if, guard does not cover" "SELECT groupArray(if(b != 0, intDiv(a, b), 0) + 1) FROM jit_short_circuit"
compare "multiIf" "SELECT groupArray(multiIf(b = 0, 0, b = -1, -a, intDiv(a, b)) + 1) FROM jit_short_circuit"
compare "multiIf, lazy condition" "SELECT groupArray(multiIf(b = 0, 0, intDiv(a, b) > 2, 1, 2) + 1) FROM jit_short_circuit WHERE k != 1"
compare "multiIf, lazy condition fails" "SELECT groupArray(multiIf(b = 1, 0, intDiv(a, b) > 2, 1, 2) + 1) FROM jit_short_circuit WHERE k != 1"
compare "multiIf, Nullable condition" "SELECT groupArray(multiIf(c, intDiv(a, b + 5), c = 0, modulo(a, 3), intDiv(a, b)) + 1) FROM jit_short_circuit WHERE k != 4"
compare "nested if" "SELECT groupArray(if(b != 0, if(b != -1, intDiv(a, b), -1), 0) + 1) FROM jit_short_circuit"
compare "and" "SELECT count() FROM jit_short_circuit WHERE b != 0 AND intDiv(a, b) > 1"
compare "and, NULL does not decide" "SELECT groupArray(n != 0 AND intDiv(a, n) > 1) FROM jit_short_circuit"
compare "or" "SELECT count() FROM jit_short_circuit WHERE b = 0 OR intDiv(a, b) > 1"
compare "or, three arguments" "SELECT groupArray(b = 0 OR b = -1 OR modulo(a, b) = 1) FROM jit_short_circuit"
compare "or, NULL does not decide" "SELECT groupArray(n = 0 OR intDiv(a, n) > 1) FROM jit_short_circuit"
compare "if in and" "SELECT count() FROM jit_short_circuit WHERE b != 0 AND if(a > 0, intDiv(a, b), 0) > 1"
compare "shared lazy node" "SELECT groupArray(if(b != 0, intDiv(a, b) + intDiv(a, b), 0) + 1) FROM jit_short_circuit WHERE k != 1"

# The guarded division ran compiled in the default mode.
$CLICKHOUSE_CLIENT $JIT --short_circuit_function_evaluation=enable --log_comment "05497_compiled_$CLICKHOUSE_DATABASE" \
    --query "SELECT sum(if(b != 0, intDiv(a, b), 0) + 1) FROM jit_short_circuit WHERE k != 1 FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'guarded division compiled', ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05497_compiled_$CLICKHOUSE_DATABASE'
        AND event_date >= yesterday()"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_short_circuit"
