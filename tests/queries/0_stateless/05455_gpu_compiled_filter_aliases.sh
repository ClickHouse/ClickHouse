#!/usr/bin/env bash

# Checks that a predicate with an alias of an expression it is of, which the analysis keeps as a node of its own, is
# compiled for the device as the same predicate without the alias is, and returns what the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, both sides of every comparison run on the CPU,
# and the plans print what they are expected to be.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_compiled_filter (k UInt8, v UInt64, x Int32, y Int32)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    -- Wide parts with every column stored plainly, which the device reads as compressed blocks.
    INSERT INTO gpu_compiled_filter
    SELECT number % 13, number, number % 1000, number % 777 - 300
    FROM numbers(300000);
"

GPU_SETTINGS=()
HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v) FROM gpu_compiled_filter" 2>&1 > /dev/null)
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

QUERY="SELECT k, count(), sum(v) FROM gpu_compiled_filter WHERE (x + y AS s) > 100 AND s < 900 GROUP BY k ORDER BY k"
compare_with_cpu "$QUERY"
check_plan "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_compiled_filter"
