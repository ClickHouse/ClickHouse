#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# compressor scratch buffers. `CompressedWriteBuffer::nextImpl` sizes its scratch buffer from the reserve of the
# stream's own codec, rounded up to a power of two by `PODArray`. With 2000000-byte blocks, the reserve of `LZ4` is
# below 2 MiB, so its scratch buffer is 2 MiB, while `FPC` needs 2125011 bytes and allocates 4 MiB. The estimate
# took the reserve of the part default codec (`LZ4`) for every stream and priced 2 MiB.
#
# The test compares the reservation of the same horizontal merge of wide parts on a local disk, with every column
# declared `CODEC(FPC)` or `CODEC(LZ4)`. The parts are large enough for the per-stream worst case to be the binding
# bound, so each `FPC` stream is priced at least 2 MiB more.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COLUMNS=10

function reserved_for_merge()
{
    local codec="$1"
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f "c%g Float64 CODEC(${codec})" 1 "$COLUMNS")
    column_values=$(seq -s ', ' -f 'number / %g' 1 "$COLUMNS")
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05475_merge_memory_reservation_stream_codec_scratch_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_codec_scratch (${column_definitions})
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 min_compress_block_size = 2000000, max_compress_block_size = 2000000,
                 default_compression_codec = 'LZ4', enable_vertical_merge_algorithm = 0,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_codec_scratch SELECT ${column_values} FROM numbers(400000);
        INSERT INTO t_merge_mem_codec_scratch SELECT ${column_values} FROM numbers(400000, 400000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_codec_scratch MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

function reserved_for_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_merge "$1")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

fpc=$(reserved_for_merge_with_retries "FPC")
lz4=$(reserved_for_merge_with_retries "LZ4")

echo "$((fpc > 0 && lz4 > 0))"
# A 4 MiB instead of a 2 MiB scratch buffer per data stream: 2097152 bytes more each.
echo "$((fpc - lz4 >= COLUMNS * 2097152))"
