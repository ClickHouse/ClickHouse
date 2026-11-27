#!/usr/bin/env bash

# Checks that an expression of integers wider than 64 bits, which the device has no instructions to convert to a float
# with, is refused rather than compiled, and returns what the CPU returns: as the argument of an aggregate, and in a
# predicate.
#
# Where the build has no GPU support, or the machine no usable device, both sides of every comparison run on the CPU,
# and the plans print what they are expected to be.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_wide_integers (k UInt8, a UInt64, b UInt64, f Float64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    INSERT INTO gpu_wide_integers SELECT number % 7, number, number * 3, number / 8 FROM numbers(100000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(a) FROM gpu_wide_integers" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --query_plan_remove_unused_columns 1)
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

compare_with_cpu "SELECT k, round(sum(toFloat64(toUInt128(a) * toUInt128(b))), 0) FROM gpu_wide_integers GROUP BY k ORDER BY k"
compare_with_cpu "SELECT k, sum(f) FROM gpu_wide_integers WHERE toFloat64(toUInt128(a) * toUInt128(b)) / 3 > 1e9 GROUP BY k ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_wide_integers"
