#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (minio)

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# compressor scratch buffers in the data-volume bound. Every stream of this horizontal merge writes 524296 bytes
# (65537 UInt64 values) through adaptive buffers that start at 512 KiB. Its flush of a full block needs a
# 1 MiB scratch buffer (CompressedWriteBuffer::nextImpl, rounded up to a power of two by PODArray), the
# compressor block grows to 1 MiB, and so does the file buffer, because the encoded block with its checksum
# does not fit into 512 KiB. Next to the 64 KiB marks compressor and the 512 KiB marks file buffer, each
# stream holds at least 3.5625 MiB. The data-volume bound priced about 3.06 MiB per stream without the scratch
# buffers.
#
# The reservation also includes the readers of the two `CODEC(NONE)` source parts: each column is read through
# its compressed and decompressed bytes, 1 MiB per column for 65537 `UInt64` values. The threshold adds them, so
# the estimate without the scratch buffers (about 4.06 MiB per column) stays below it.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COLUMNS=20

function reserved_for_merge()
{
    local tag="$1"
    local column_definitions column_values data_dir
    column_definitions=$(seq -s ', ' -f 'c%g UInt64 CODEC(NONE)' 1 "$COLUMNS")
    column_values=$(seq -s ', ' -f 'number * %g' 1 "$COLUMNS")
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05444_merge_memory_reservation_compression_scratch_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_scratch (${column_definitions})
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(
                     type = s3,
                     endpoint = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_${tag}/',
                     access_key_id = 'clickhouse',
                     secret_access_key = 'clickhouse'),
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 min_compress_block_size = 1048576, max_compress_block_size = 1048576,
                 min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 524288,
                 marks_compress_block_size = 65536, enable_vertical_merge_algorithm = 0,
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_scratch SELECT ${column_values} FROM numbers(32768);
        INSERT INTO t_merge_mem_scratch SELECT ${column_values} FROM numbers(32768, 32769);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_scratch MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

result=0
for attempt in 1 2 3
do
    result=$(reserved_for_merge "$attempt")
    result=${result:-0}
    if [ "$result" -gt 0 ]; then break; fi
done

echo "$((result > 0))"
# 3.5625 MiB = 3735552 bytes per stream of the writer and 1 MiB = 1048576 bytes per column of the readers.
echo "$((result >= COLUMNS * (3735552 + 1048576)))"
