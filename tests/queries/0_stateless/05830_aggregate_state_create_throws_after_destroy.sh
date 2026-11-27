#!/usr/bin/env bash

# An aggregate state was destroyed twice when its `create` threw right after the previous state was destroyed:
# in `recreate` of window aggregate states, and in `-ArgMin` / `-ArgMax` when the nested state is reset.
# `-Resample` makes most of the allocations happen in `create`, and only the last bucket keeps data that is freed
# twice. `-OrDefault` makes the results of the empty buckets cheap.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="SETTINGS allow_introspection_functions = 1, memory_tracker_fault_probability = 0.0001, max_untracked_memory = 1"

function check()
{
    local name=$1 query=$2 expected=$3
    local unexpected=0
    for _ in {1..10}; do
        out=$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "$query $SETTINGS" 2>&1)
        if [ "$out" != "$expected" ] && ! grep -q "MEMORY_LIMIT_EXCEEDED" <<< "$out"; then
            echo "$name: unexpected output: $out"
            unexpected=1
        fi
    done
    [ $unexpected -eq 0 ] && echo "$name: OK"
}

check window "SELECT count() FROM (SELECT flameGraphOrDefaultResample(0, 100, 1)([toUInt64(1)], 99) OVER (PARTITION BY number) AS r FROM numbers(5000))" 5000
check argMax "SELECT length(flameGraphResampleArgMax(0, 100, 1)([toUInt64(1)], 99, number)) FROM numbers(3000)" 100

# The server is alive.
$CLICKHOUSE_CLIENT -q "SELECT 1"
