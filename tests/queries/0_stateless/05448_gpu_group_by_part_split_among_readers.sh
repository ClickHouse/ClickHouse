#!/usr/bin/env bash

# Checks a GPU `GROUP BY` and a GPU-filtered read over a table of one large part with several readers: the part is read
# in pieces of ranges of marks, one per reader, and the rows of the pieces group as those of the part. The results are
# those of the CPU. Where the build has no GPU support, or the machine no usable device, the settings are not applied
# and both sides of every comparison run on the CPU.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_split (k UInt32, d Date, v UInt64, f Float64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192;
    SYSTEM STOP MERGES gpu_split;
    INSERT INTO gpu_split SELECT number, toDate('1995-01-01') + number % 1000, number * 7, number / 9 FROM numbers(3000000)
    SETTINGS max_insert_threads = 1, max_insert_block_size = 10000000, min_insert_block_size_rows = 10000000, min_insert_block_size_bytes = 0;
"

echo "parts: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'gpu_split' AND active")"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT k % 10, sum(v) FROM gpu_split GROUP BY k % 10 FORMAT Null" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --allow_experimental_gpu_filtered_read 1 --gpu_aggregation_readers 4 --gpu_aggregation_decompression device)
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

compare_with_cpu "SELECT d, count(), sum(v), min(k), max(k) FROM gpu_split GROUP BY d ORDER BY d LIMIT 5"
compare_with_cpu "SELECT count(), sum(v), sum(cityHash64(d)) FROM (SELECT d, sum(v) AS v FROM gpu_split GROUP BY d)"
compare_with_cpu "SELECT k % 7 AS m, count(), sum(v), round(sum(f), 3) FROM gpu_split WHERE d >= toDate('1995-03-01') AND d < toDate('1995-04-01') GROUP BY m ORDER BY m"
compare_with_cpu "SELECT count(), sum(v), round(sum(f), 3), min(k), max(k) FROM gpu_split WHERE d = toDate('1996-02-02') AND v % 2 = 0"
compare_with_cpu "SELECT k, v FROM gpu_split WHERE d = toDate('1997-07-07') AND k > 1000000 ORDER BY k LIMIT 3"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_split"
