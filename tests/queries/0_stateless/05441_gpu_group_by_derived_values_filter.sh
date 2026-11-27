#!/usr/bin/env bash

# Checks a `GROUP BY` over compressed blocks whose aggregates take `Float64` arithmetic of columns,
# which the device computes, with and without a `WHERE` the device evaluates, against the CPU. A
# batch of which the `WHERE` leaves out few rows keeps them as nulls of its first key, and one of
# which it leaves out many is compacted to the rows it keeps before their arithmetic is computed and
# they are grouped: the `WHERE`s below keep all, most, a third, a hundredth and none of the rows.
#
# The floats are multiples of an eighth, a quarter and a half, so that their products and the sums
# of those are exact in a double in any order of addition.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_derived_filter (k UInt32, k_small UInt8, f UInt16, p Float64, d Float64, t Float64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    SYSTEM STOP MERGES gpu_derived_filter;
"

for part in 0 1 2; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO gpu_derived_filter
        SELECT number % 50000, number % 251, number % 60000, (number % 1000) / 8, (number % 4) / 4, (number % 2) / 2
        FROM numbers($part * 400000, 400000 + $part)"
done

GPU_SETTINGS=()
HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(p) FROM gpu_derived_filter" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --optimize_syntax_fuse_functions 0)
        HAS_GPU=1
    elif [[ "$PROBE_ERROR" != *"Cannot aggregate on a GPU"* ]]; then
        echo "$PROBE_ERROR"
    fi
fi

# The device is offered a `PREWHERE` only, and the test runner may have turned off the setting that
# makes one of a `WHERE` - as a client option, which a later client option does not override, so
# the query itself has to say it.
function compare_with_cpu()
{
    local query="$1 SETTINGS optimize_move_to_prewhere = 1${2:+, $2}"
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

# Whether the device reads the compressed blocks and evaluates the `WHERE`. On a machine without a
# device the plan is the CPU's, and the expected line is printed as it is.
function check_plan()
{
    local plan
    if [ "$HAS_GPU" == "0" ]; then
        echo "compressed, filter on the device"
        return
    fi

    plan=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $1 SETTINGS optimize_move_to_prewhere = 1")
    if [[ "$plan" == *"ReadFromGPUCompressedColumns"* && "$plan" == *"Filter on the device"* ]]; then
        echo "compressed, filter on the device"
    else
        echo "elsewhere"
    fi
}

check_plan "SELECT k_small, sum(p * (1 - d)) FROM gpu_derived_filter WHERE f < 600 GROUP BY k_small"

for mode in device host; do
    echo "- $mode"
    for where in "" "WHERE f >= 1000" "WHERE f >= 12000" "WHERE f < 20000" "WHERE f < 600" "WHERE f > 60000"; do
        compare_with_cpu "
            SELECT k_small, sum(p * (1 - d)), sum(p * (1 - d) * (1 + t)), sum(p), count()
            FROM gpu_derived_filter $where GROUP BY k_small ORDER BY k_small" "gpu_aggregation_decompression = '$mode'"
    done

    # Many groups, of which a `WHERE` keeps few rows each, and an `avg`.
    compare_with_cpu "
        SELECT k, sum(p * (1 - d)), avg(p), count() FROM gpu_derived_filter WHERE f < 3000 GROUP BY k ORDER BY k" \
        "gpu_aggregation_decompression = '$mode'"

    # Small batches from several readers, so that the device thread hands many of them over to the
    # grouping thread while it groups.
    compare_with_cpu "
        SELECT k_small, sum(p * (1 - d) * (1 + t)), count() FROM gpu_derived_filter WHERE f < 20000 GROUP BY k_small ORDER BY k_small" \
        "gpu_aggregation_decompression = '$mode', gpu_aggregation_batch_bytes = 65536, gpu_aggregation_readers = 3"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_derived_filter"
