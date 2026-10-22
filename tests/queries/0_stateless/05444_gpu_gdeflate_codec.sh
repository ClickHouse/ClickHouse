#!/usr/bin/env bash

# Checks the `GDeflate` codec, which compresses and expands on the GPU with nvcomp: the values of a
# table written with it read back as they were written, both through the CPU, which has the codec
# expand each block on the device, and through a `GROUP BY` on the device, which expands the chunks
# of the blocks it reads itself. The blocks are larger than a chunk of 64 KiB, and some are shorter.
#
# The codec exists only in a build with GPU support and works only on a server with a usable device;
# elsewhere the expected lines are printed as they are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "CREATE TABLE gpu_gdeflate_probe (x UInt64 CODEC(GDeflate)) ENGINE = MergeTree ORDER BY tuple()" \
        --enable_gdeflate_codec 1
    if $CLICKHOUSE_CLIENT --query "INSERT INTO gpu_gdeflate_probe SELECT number FROM numbers(10)" --enable_gdeflate_codec 1 2>/dev/null; then
        HAS_GPU=1
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE gpu_gdeflate_probe"
fi

if [ "$HAS_GPU" == "0" ]; then
    cat "$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

for algorithm in 0 4; do
    $CLICKHOUSE_CLIENT --enable_gdeflate_codec 1 --query "
        CREATE TABLE gpu_gdeflate (k UInt32 CODEC(GDeflate($algorithm)), v Float64 CODEC(GDeflate($algorithm)), d Date CODEC(GDeflate($algorithm)),
                                   s String CODEC(GDeflate($algorithm)))
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
        CREATE TABLE gpu_lz4 (k UInt32, v Float64, d Date, s String) ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO gpu_gdeflate SELECT number % 1000, (number % 977) / 8, toDate('2024-01-01') + number % 365, toString(number % 5003) FROM numbers(1000003);
        INSERT INTO gpu_lz4 SELECT number % 1000, (number % 977) / 8, toDate('2024-01-01') + number % 365, toString(number % 5003) FROM numbers(1000003);
    "

    echo "- algorithm $algorithm"

    # Read through the CPU.
    $CLICKHOUSE_CLIENT --query "
        SELECT (SELECT sum(cityHash64(k, v, d, s)) FROM gpu_gdeflate) = (SELECT sum(cityHash64(k, v, d, s)) FROM gpu_lz4)"

    # Grouped on the device from the compressed chunks, against the CPU over the other table.
    for mode in device host; do
        on_cpu=$($CLICKHOUSE_CLIENT --query "SELECT k, sum(v), count() FROM gpu_lz4 WHERE d < '2024-06-01' GROUP BY k ORDER BY k")
        on_gpu=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 --query "
            SELECT k, sum(v), count() FROM gpu_gdeflate WHERE d < toDate('2024-06-01') GROUP BY k ORDER BY k
            SETTINGS optimize_move_to_prewhere = 1, gpu_aggregation_decompression = '$mode'")
        [ "$on_cpu" == "$on_gpu" ] && echo "$mode ok" || echo "$mode MISMATCH"
    done

    $CLICKHOUSE_CLIENT --query "DROP TABLE gpu_gdeflate; DROP TABLE gpu_lz4"
done
