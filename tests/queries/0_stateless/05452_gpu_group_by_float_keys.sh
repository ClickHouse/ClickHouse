#!/usr/bin/env bash

# Checks a `GROUP BY` on the device by `Float32` and `Float64` keys against the CPU. ClickHouse groups floats by their
# bits: `0` and `-0` are two groups, and so are NaNs of different payloads; the device groups a float key as the integer
# of its bits. The keys are alone and with another, over the blocks the pipeline reads and over compressed columns, and
# of a `GROUP BY` over a join, which the device aggregates too.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied and both sides of every
# comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NaNs of both signs and of other payloads than the canonical one, zeros of both signs, infinities and ordinary values.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_float_keys (d Float64, f Float32, k UInt8, v UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    CREATE TABLE gpu_float_join (k UInt8, w UInt64) ENGINE = MergeTree ORDER BY k;

    INSERT INTO gpu_float_keys SELECT
        [0., -0., nan, -nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18446744073709551615)),
         inf, -inf, 1.5, -1.5, number / 1000][number % 11 + 1],
        [toFloat32(0.), toFloat32(-0.), toFloat32(nan), toFloat32(-nan), reinterpretAsFloat32(toUInt32(2143289345)),
         toFloat32(inf), toFloat32(-inf), toFloat32(2.25), toFloat32(number % 77)][number % 9 + 1],
        number % 5,
        number
    FROM numbers(200003);

    INSERT INTO gpu_float_join SELECT number, number * 10 FROM numbers(5);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one rather than being
    # compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT k, sum(v) FROM gpu_float_keys GROUP BY k" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --query_plan_remove_unused_columns 1)
    elif [[ "$PROBE_ERROR" != *"Cannot aggregate on a GPU"* ]]; then
        echo "$PROBE_ERROR"
    fi
fi

function compare_with_cpu()
{
    local query="$1"
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

# Whether the device aggregates, by the step `EXPLAIN` names.
function on_device()
{
    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "device"
    elif [[ "$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN $1")" == *GPU* ]]; then
        echo "device"
    else
        echo "cpu"
    fi
}

# The groups as the bits of their keys, so that `0` and `-0` and the NaNs of different payloads are told apart.
BITS_D="reinterpretAsUInt64(d)"
BITS_F="reinterpretAsUInt32(f)"

for settings in "" "SETTINGS gpu_aggregation_batch_bytes = 65536" "SETTINGS optimize_move_to_prewhere = 0"; do
    echo "- ${settings:-default}"
    on_device "SELECT d, sum(v) FROM gpu_float_keys GROUP BY d $settings"
    compare_with_cpu "SELECT $BITS_D AS b, sum(v), count() FROM gpu_float_keys GROUP BY d ORDER BY b $settings"
    compare_with_cpu "SELECT $BITS_F AS b, sum(v), min(k) FROM gpu_float_keys GROUP BY f ORDER BY b $settings"
    compare_with_cpu "SELECT $BITS_D AS b, k, sum(v) FROM gpu_float_keys GROUP BY d, k ORDER BY b, k $settings"
    compare_with_cpu "SELECT $BITS_F AS b, $BITS_D AS c, count() FROM gpu_float_keys GROUP BY f, d ORDER BY b, c $settings"
    # The pipeline's blocks, as a filter between the read and the aggregation keeps the device to them.
    compare_with_cpu "SELECT $BITS_D AS b, sum(v) FROM gpu_float_keys WHERE v % 3 = 0 GROUP BY d ORDER BY b $settings"
done

# A `GROUP BY` over a join, which the device aggregates as well.
QUERY="SELECT j.w, $BITS_D AS b, sum(v) FROM gpu_float_keys AS t INNER JOIN gpu_float_join AS j ON t.k = j.k GROUP BY j.w, t.d ORDER BY j.w, b"
on_device "$QUERY"
compare_with_cpu "$QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_float_keys; DROP TABLE gpu_float_join"
