#!/usr/bin/env bash

# Checks a `GROUP BY` on the device by the key a table is sorted by against the CPU. The device
# reduces the rows of a batch over their runs of equal keys before it groups them, where the
# first batch has few runs per row and every aggregate combines over runs: a `sum`, a `min`, a `max`
# and a `count`. A key is in several parts, and with small batches in several batches of one part,
# so that its runs come to one group. Keys whose rows are not together, a second key that splits
# the runs and an `avg` keep the device to the rows as they are.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_runs (k UInt32, k2 UInt8, f UInt16, v UInt64, i Int32)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    SYSTEM STOP MERGES gpu_runs;
"

for part in 0 1 2; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO gpu_runs
        SELECT intDiv(number, 4) % 100000, number % 3, number % 60000, cityHash64(number) % 1000000, toInt32(number % 100000) - 50000
        FROM numbers($part * 500000, 500000 + $part)"
done

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v) FROM gpu_runs" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1)
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

# Whether the device aggregates. On a machine without a device the plan is the CPU's, and the
# expected line is printed as it is.
if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
    echo "device"
elif [[ "$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN SELECT k, sum(v) FROM gpu_runs GROUP BY k")" == *"GPUAggregating"* ]]; then
    echo "device"
else
    echo "elsewhere"
fi

for settings in "" "gpu_aggregation_batch_bytes = 65536"; do
    echo "- ${settings:-default batches}"

    # By the sorting key, with and without a `WHERE`, and a `HAVING` as in TPC-H Q18.
    compare_with_cpu "SELECT k, sum(v), min(i), max(i), count() FROM gpu_runs GROUP BY k ORDER BY k" "$settings"
    compare_with_cpu "SELECT k, sum(v), count() FROM gpu_runs WHERE f < 20000 GROUP BY k ORDER BY k" "$settings"
    compare_with_cpu "SELECT k, max(v), min(i) FROM gpu_runs WHERE f >= 1000 GROUP BY k ORDER BY k" "$settings"
    compare_with_cpu "SELECT k FROM gpu_runs GROUP BY k HAVING sum(v) > 9000000 ORDER BY k" "$settings"

    # Rows of a key not together, a second key that splits the runs, and an `avg`.
    compare_with_cpu "SELECT f, sum(v), count() FROM gpu_runs GROUP BY f ORDER BY f" "$settings"
    compare_with_cpu "SELECT k, k2, sum(v), count() FROM gpu_runs GROUP BY k, k2 ORDER BY k, k2" "$settings"
    compare_with_cpu "SELECT k, avg(i), count() FROM gpu_runs GROUP BY k ORDER BY k" "$settings"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_runs"
