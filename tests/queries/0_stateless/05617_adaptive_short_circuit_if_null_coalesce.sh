#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ifNull`, `coalesce` and `caseWithExpression` execute their lazy arguments themselves, so they must report the profiles of
# these arguments, as `if` does. Otherwise `enable_adaptive_short_circuit_lazy_execution` never sees how
# much a lazy argument costs and never switches it to eager execution. Here the lazily executed `and`
# is needed for 99% of the rows and is so cheap that filtering and expanding the rows costs more than
# executing it on every row. `AdaptiveShortCircuitEagerExecutions` counts the actions which the heuristic
# decided to execute eagerly, so it stays zero if the profiles of the lazy arguments are lost.

SETTINGS="SETTINGS short_circuit_function_evaluation = 'force_enable', max_block_size = 8192, max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0, compile_aggregate_expressions = 0"

function check()
{
    local name="$1"
    local query="$2"

    # The result must not depend on the adaptive decisions.
    local static_result
    static_result=$(${CLICKHOUSE_CLIENT} --query "$query $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 0")

    # The decision is based on wall-clock measurements, so on a heavily loaded machine an unlucky run can
    # keep every action lazy. Retry a few times: if the profiles of the lazy arguments are lost, no action
    # is ever switched to eager execution, so the check still catches that.
    local adaptive_result
    local results_match=1
    local switched=0
    for attempt in {1..5}
    do
        local query_id="${CLICKHOUSE_DATABASE}_${name}_${attempt}"
        adaptive_result=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$query $SETTINGS, enable_adaptive_short_circuit_lazy_execution = 1")
        [ "$adaptive_result" = "$static_result" ] || results_match=0

        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        switched=$(${CLICKHOUSE_CLIENT} --query "
            SELECT ProfileEvents['AdaptiveShortCircuitEagerExecutions'] > 0
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'")
        [ "$switched" = "1" ] && break
    done
    echo -e "$name\t$static_result\t$results_match\t$switched"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t (n Nullable(UInt8), b1 UInt8, b2 UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t SELECT if(number % 100 = 0, toNullable(toUInt8(1)), NULL), number % 3 != 0, number % 5 != 0 FROM numbers(1000000);"

check "ifNull" "SELECT sum(ifNull(n, and(b1, b2))) FROM t"
check "coalesce" "SELECT sum(coalesce(n, and(b1, b2))) FROM t"
# The constant `NULL` arguments are skipped by `coalesce`, the profiles must still refer to the positions of the arguments.
check "coalesce_with_nulls" "SELECT sum(coalesce(NULL, n, NULL, n, and(b1, b2))) FROM t"
# `CASE x WHEN ... END` with constant values uses `transform`, and `multiIf` otherwise.
check "case_transform" "SELECT sum(CASE isNull(n) WHEN 0 THEN 7 ELSE and(b1, b2) END) FROM t"
check "case_multi_if" "SELECT sum(CASE isNull(n) WHEN 0 THEN 7 WHEN b1 + 100 THEN 8 ELSE and(b1, b2) END) FROM t"
check "if" "SELECT sum(if(isNull(n), and(b1, b2), assumeNotNull(n))) FROM t"
