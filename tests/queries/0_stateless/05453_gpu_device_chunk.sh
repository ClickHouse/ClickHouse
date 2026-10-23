#!/usr/bin/env bash

# Checks the chunks whose columns stay on the device between steps: a read filtered on the device hands its rows to the
# steps above there, an `ExpressionTransform` computes what the device can over them, and a GPU aggregation without keys
# reduces them where they are. Any other step, or an expression the device cannot compute, has the columns in host
# memory as it pulls them. The results are those of the CPU in each of these cases.
#
# The float columns hold multiples of an eighth, so that the sums are exact in a double in any order of addition.
#
# Where the build has no GPU support, or the machine no usable device, the settings are not applied and both sides of
# every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The statistics the device's share of the read is decided by are made as the rows are inserted, whatever the test
# harness may set.
$CLICKHOUSE_CLIENT --materialize_statistics_on_insert 1 --query "
    CREATE TABLE gpu_device_chunk (k UInt32, d Date, m String, p Float64, e Float64, q Int64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
        serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
        auto_statistics_types = 'basic, uniq_v2';

    INSERT INTO gpu_device_chunk
    SELECT number, toDate('1992-01-01') + number % 2500, ['MAIL', 'SHIP', 'AIR', 'RAIL'][number % 4 + 1],
           (number % 100000) / 8, (number % 11) / 8, number % 50
    FROM numbers(500009);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one rather than being
    # compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_filtered_read 1 \
        --query "SELECT count() FROM gpu_device_chunk WHERE d < '1993-01-01' AND q < 24" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_filtered_read 1 --allow_experimental_gpu_aggregation 1 --optimize_move_to_prewhere 1 --use_statistics 1 --query_plan_remove_unused_columns 1)
    elif [[ "$PROBE_ERROR" != *"There is no GPU"* ]]; then
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
        echo "MISMATCH for '$query': $on_cpu on the CPU, $on_gpu on the GPU"
    fi
}

WHERE="WHERE d >= '1994-01-01' AND d < '1995-01-01' AND e BETWEEN 0.25 AND 0.75 AND q < 24"

# Of the shape of TPC-H Q6: the product computed and summed on the device.
compare_with_cpu "SELECT sum(p * e) FROM gpu_device_chunk $WHERE"
# Several aggregates over the columns as they are read and computed.
compare_with_cpu "SELECT sum(p), sum(e), min(p), max(p * e - p / 2) FROM gpu_device_chunk $WHERE"
# Several threads and small blocks, so that many chunks pass on the device.
compare_with_cpu "SELECT sum(p * e) FROM gpu_device_chunk $WHERE SETTINGS max_threads = 8, max_block_size = 1000"
# An expression over a column the device does not compute from: in host memory before it.
compare_with_cpu "SELECT sum(p * k) FROM gpu_device_chunk $WHERE"
# Rows to steps that know nothing of the device: a sort and a `GROUP BY` with keys.
compare_with_cpu "SELECT k, m, p * e FROM gpu_device_chunk $WHERE ORDER BY k LIMIT 5"
compare_with_cpu "SELECT m, count(), sum(p * e) FROM gpu_device_chunk $WHERE GROUP BY m ORDER BY m"

# Of the shape of TPC-H Q6, no chunk is copied to host memory before the aggregation.
if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
    echo "on the device"
else
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_q6"
    $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query_id "$query_id" --format Null --query "SELECT sum(p * e) FROM gpu_device_chunk $WHERE"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['DeviceChunksMaterialized'] = 0 ? 'on the device' : 'copied to host memory'
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_device_chunk"
