#!/usr/bin/env bash

# Checks a `GROUP BY` over compressed blocks that reads the ranges of marks the primary key and a
# skip index leave of a part, against the CPU. The device reads whole compressed blocks from the one
# a range starts in and leaves the rows outside the range out of them: the blocks of the table hold
# many granules each, so that the ranges start and end in the middle of a block, and there are
# several ranges in a part. A skip index is applied to the ranges whether the read would apply it
# to the granules it reads or not.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_ranges (k UInt32, v UInt32, x UInt16, f Float64, b UInt8, d Date, INDEX iv v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
        index_granularity = 1000, index_granularity_bytes = 0, min_compress_block_size = 1000000, max_compress_block_size = 1000000;

    SYSTEM STOP MERGES gpu_ranges;
"

# The second part's skip index leaves ranges that do not follow the key.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO gpu_ranges
    SELECT number, number, number % 1000, (number % 64) / 8, number % 7, toDate('2024-01-01') + number % 100 FROM numbers(500000)"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO gpu_ranges
    SELECT number, (number * 7) % 500000, number % 1000, (number % 64) / 8, number % 7, toDate('2024-01-01') + number % 100
    FROM numbers(500000)"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(k) FROM gpu_ranges" 2>&1 > /dev/null)
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

# Whether the device reads the parts in ranges of marks. On a machine without a device the plan is
# the CPU's, and the expected line is printed as it is. A read that keeps the column of its filter,
# which nothing above uses, keeps the aggregation on the CPU, so the plan says to drop it.
function check_plan()
{
    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "in ranges of marks"
    elif [[ "$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $1 SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1")" == *"Parts read in ranges of marks"* ]]; then
        echo "in ranges of marks"
    else
        echo "elsewhere"
    fi
}

check_plan "SELECT x, sum(k) FROM gpu_ranges WHERE k < 50000 GROUP BY x"
check_plan "SELECT x, sum(k) FROM gpu_ranges WHERE v < 50000 GROUP BY x"

for mode in device host; do
    for on_data_read in 0 1; do
        settings="gpu_aggregation_decompression = '$mode', use_skip_indexes_on_data_read = $on_data_read"
        echo "- $mode, use_skip_indexes_on_data_read = $on_data_read"

        # By the primary key: one range per part, two, and one that starts and ends inside a block.
        compare_with_cpu "SELECT x, sum(k), count(), max(f), min(b), max(d) FROM gpu_ranges WHERE k < 50000 GROUP BY x ORDER BY x" "$settings"
        compare_with_cpu "SELECT x, sum(k), count() FROM gpu_ranges WHERE k < 30000 OR k > 470000 GROUP BY x ORDER BY x" "$settings"
        compare_with_cpu "SELECT b, sum(f), count() FROM gpu_ranges WHERE k >= 123456 AND k < 234567 GROUP BY b ORDER BY b" "$settings"

        # By the skip index, alone and with the primary key, and with a filter left to the device.
        compare_with_cpu "SELECT x, sum(k), count() FROM gpu_ranges WHERE v < 50000 GROUP BY x ORDER BY x" "$settings"
        compare_with_cpu "SELECT b, sum(v), count() FROM gpu_ranges WHERE v >= 200000 AND v < 210000 AND k < 400000 GROUP BY b ORDER BY b" "$settings"
        compare_with_cpu "SELECT x, sum(k), count() FROM gpu_ranges WHERE k < 300000 AND b = 3 GROUP BY x ORDER BY x" "$settings"

        # Small batches from several readers.
        compare_with_cpu "SELECT x, sum(k), count() FROM gpu_ranges WHERE k < 30000 OR k > 470000 GROUP BY x ORDER BY x" \
            "$settings, gpu_aggregation_batch_bytes = 65536, gpu_aggregation_readers = 3"
    done
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_ranges"
