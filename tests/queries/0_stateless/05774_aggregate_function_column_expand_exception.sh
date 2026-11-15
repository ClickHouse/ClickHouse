#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fault injection in the memory tracker makes the test slow.

# `ColumnAggregateFunction::expand` (used by the short-circuit evaluation of `if`) must stay valid when
# creating an aggregate function state throws, so that the column can be destroyed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for _ in $(seq 1 30); do
    $CLICKHOUSE_CLIENT --query "
        SELECT finalizeAggregation(if(number % 3 = 0,
            initializeAggregation('groupArrayState', number),
            initializeAggregation('groupArrayState', number + 1)))
        FROM numbers(10000)
        FORMAT Null
        SETTINGS short_circuit_function_evaluation = 'force_enable', memory_tracker_fault_probability = 0.001, max_untracked_memory = 0, max_block_size = 1000
    " >/dev/null 2>&1
done

$CLICKHOUSE_CLIENT --query "SELECT 'ok'"
