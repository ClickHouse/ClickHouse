#!/usr/bin/env bash

# Checks that a predicate over more columns than a kernel took before, 20, is compiled for the device, and returns what
# the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, both sides of every comparison run on the CPU,
# and the plans print what they are expected to be.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_many_columns (k UInt8, v UInt64, c0 UInt16, c1 UInt16, c2 UInt16, c3 UInt16, c4 UInt16, c5 UInt16, c6 UInt16, c7 UInt16, c8 UInt16, c9 UInt16, c10 UInt16, c11 UInt16, c12 UInt16, c13 UInt16, c14 UInt16, c15 UInt16, c16 UInt16, c17 UInt16, c18 UInt16, c19 UInt16)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    INSERT INTO gpu_many_columns SELECT number % 13, number, (number * 3) % 1000, (number * 4) % 1000, (number * 5) % 1000, (number * 6) % 1000, (number * 7) % 1000, (number * 8) % 1000, (number * 9) % 1000, (number * 10) % 1000, (number * 11) % 1000, (number * 12) % 1000, (number * 13) % 1000, (number * 14) % 1000, (number * 15) % 1000, (number * 16) % 1000, (number * 17) % 1000, (number * 18) % 1000, (number * 19) % 1000, (number * 20) % 1000, (number * 21) % 1000, (number * 22) % 1000 FROM numbers(300000);
"

GPU_SETTINGS=()
HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v) FROM gpu_many_columns" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --query_plan_remove_unused_columns 1)
        HAS_GPU=1
    elif [[ "$PROBE_ERROR" != *"Cannot aggregate on a GPU"* ]]; then
        echo "$PROBE_ERROR"
    fi
fi

# The device is offered a `PREWHERE` only, which the test runner may have turned off.
SETTINGS="SETTINGS optimize_move_to_prewhere = 1"

function compare_with_cpu()
{
    local query="$1 $SETTINGS"
    local on_cpu
    local on_gpu

    on_cpu=$($CLICKHOUSE_CLIENT --query "$query")
    on_gpu=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "$query")

    if [ "$on_cpu" == "$on_gpu" ]; then
        echo "ok"
    else
        echo "MISMATCH for '$query'"
    fi
}

function check_plan()
{
    local query="$1 $SETTINGS"

    if [ "$HAS_GPU" == "0" ]; then
        echo "filter on the device"
    elif $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $query" | grep -q "Filter on the device"; then
        echo "filter on the device"
    else
        echo "filter on the CPU"
    fi
}

QUERY="SELECT k, count(), sum(v) FROM gpu_many_columns WHERE c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 > 10000 GROUP BY k ORDER BY k"
compare_with_cpu "$QUERY"
check_plan "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_many_columns"
