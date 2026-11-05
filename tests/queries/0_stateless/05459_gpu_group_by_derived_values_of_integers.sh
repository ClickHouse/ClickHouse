#!/usr/bin/env bash

# Checks the arguments of aggregates that the device computes into other types than `Float64`, from columns of other
# types than `Float64`: each is computed on the device and returns what the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, both sides of every comparison run on the CPU,
# and the plans print what they are expected to be.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_derived_integers (k UInt8, a Int32, b UInt16, c Int64, f Float64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    INSERT INTO gpu_derived_integers
    SELECT number % 13, number % 1000 - 500, number % 60000, number * 7 - 1000000, number / 8 FROM numbers(300000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(a) FROM gpu_derived_integers" 2>&1 > /dev/null)
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

function computed_on_the_device()
{
    local query="$1 $SETTINGS"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "computed on the device"
    elif $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $query" | grep -q "Computed on the device"; then
        echo "computed on the device"
    else
        echo "computed on the CPU"
    fi
}

for query in \
    "SELECT k, sum(a * b) FROM gpu_derived_integers GROUP BY k ORDER BY k" \
    "SELECT k, min(a + c), max(b - a) FROM gpu_derived_integers GROUP BY k ORDER BY k" \
    "SELECT k, sum(c * 3 + a), sum(f) FROM gpu_derived_integers WHERE b < 30000 GROUP BY k ORDER BY k"
do
    compare_with_cpu "$query"
    computed_on_the_device "$query"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_derived_integers"
