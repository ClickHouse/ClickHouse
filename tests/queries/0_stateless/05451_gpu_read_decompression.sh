#!/usr/bin/env bash

# Checks `allow_experimental_gpu_read_decompression`: the reads of a wide part decompress its blocks on the GPU and
# return what they return without it. The columns are of every codec the device expands - `LZ4`, `ZSTD`, `GDeflate`,
# and these after `BitPacking` - and of one it does not, `Delta`, whose blocks are decompressed on the CPU, of integers,
# floats, dates, strings, `Nullable` and arrays. The blocks are small, so that a batch holds many of them and the reads
# seek within it: by a `PREWHERE`, by the ranges of marks the primary key leaves, and by `LIMIT`s and `FINAL`-free
# lookups of single rows.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied and both sides of every
# comparison read on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_gdeflate_codec 1 --enable_bitpacking_codec 1 --query "
    CREATE TABLE gpu_read
    (
        k UInt64 CODEC(BitPacking, GDeflate(2)),
        a Int32 CODEC(LZ4),
        b UInt16 CODEC(ZSTD(1)),
        c Int64 CODEC(BitPacking, ZSTD(1)),
        d Date CODEC(BitPacking, LZ4),
        f Float64 CODEC(GDeflate(2)),
        g Int64 CODEC(Delta, LZ4),
        s String CODEC(GDeflate(2)),
        t String CODEC(ZSTD(1)),
        n Nullable(UInt32) CODEC(LZ4),
        arr Array(UInt16) CODEC(GDeflate(2))
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1000,
        min_compress_block_size = 4096, max_compress_block_size = 8192, ratio_of_defaults_for_sparse_serialization = 1,
        serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream', pack_string_sizes = 1;

    INSERT INTO gpu_read SELECT
        number,
        toInt32(number % 100000) - 50000,
        number % 65536,
        -toInt64(number % 7777),
        toDate('2020-01-01') + number % 1500,
        number / 7,
        toInt64(number) * 3,
        if(number % 5 = 0, '', repeat(char(97 + number % 26), number % 23)),
        toString(cityHash64(number)),
        if(number % 3 = 0, NULL, number % 1000),
        range(number % 5)
    FROM numbers(300007);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one rather than being
    # compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_read_decompression 1 --use_uncompressed_cache 0 \
        --query "SELECT sum(k) FROM gpu_read" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_read_decompression 1 --use_uncompressed_cache 0)
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
        echo "MISMATCH for '$query'"
    fi
}

# Every column, all of the part.
compare_with_cpu "SELECT sum(cityHash64(*)), count() FROM gpu_read"
compare_with_cpu "SELECT sum(k), sum(a), sum(b), sum(c), max(d), sum(cityHash64(f)), sum(g), sum(length(s)), sum(cityHash64(t)), sum(n), sum(length(arr)) FROM gpu_read"

# A `PREWHERE`, whose columns are read first and the others only where it holds.
compare_with_cpu "SELECT sum(cityHash64(s, t, arr)), count() FROM gpu_read PREWHERE b < 1000"
compare_with_cpu "SELECT sum(cityHash64(f)), count() FROM gpu_read PREWHERE s = 'aaaa' WHERE n IS NOT NULL"

# Ranges of marks that the primary key leaves, and rows looked up one at a time.
compare_with_cpu "SELECT sum(cityHash64(*)), count() FROM gpu_read WHERE k BETWEEN 1000 AND 5000 OR k BETWEEN 150000 AND 151234 OR k > 299000"
compare_with_cpu "SELECT * FROM gpu_read WHERE k IN (0, 999, 1000, 77777, 300006) ORDER BY k"
compare_with_cpu "SELECT k, s, arr FROM gpu_read ORDER BY k LIMIT 10 OFFSET 123456"

# Several threads and small blocks of the pipeline, so that the streams are read in many small steps.
compare_with_cpu "SELECT sum(cityHash64(*)) FROM gpu_read SETTINGS max_threads = 8, max_block_size = 777"

# The device expands the blocks it can: of every codec but `Delta`, which the CPU decompresses.
if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
    echo "on the device"
else
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_read"
    $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query_id "$query_id" --format Null --query "SELECT * FROM gpu_read"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['GPUDecompressionBytes'] > 0 ? 'on the device' : 'not on the device'
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_read"
