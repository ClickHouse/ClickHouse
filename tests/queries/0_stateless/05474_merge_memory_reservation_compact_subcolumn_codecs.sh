#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of the
# compressors of a compact part writer. `MergeTreeDataPartWriterCompact` keeps one `CompressedWriteBuffer` of
# `DBMS_DEFAULT_BUFFER_SIZE` (1 MiB) per distinct resolved codec, and resolves the codec of every stream with
# `getSubstreamCodecDescription`, so a codec declared for a subcolumn (`CODEC p.a (...)`) applies to the stream of
# that subcolumn. The estimate counted the compressors from the column codecs only, so twenty tuple elements with
# distinct subcolumn codecs counted as one compressor.
#
# The test compares the reservation of the same merge of tiny compact parts with and without distinct subcolumn
# codecs: twenty distinct codecs open nineteen more compressors, at least 19 MiB more.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ELEMENTS=20

function reserved_for_merge()
{
    local subcolumn_codecs="$1"
    local elements element_values data_dir
    elements=$(seq -s ', ' -f 'a%g UInt64' 1 "$ELEMENTS")
    element_values=$(seq -s ', ' -f 'number * %g' 1 "$ELEMENTS")
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05474_merge_memory_reservation_compact_subcolumn_codecs_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" --enable_subcolumn_codecs 1 -q "
        CREATE TABLE t_merge_mem_subcolumn_codecs (p Tuple(${elements}) ${subcolumn_codecs})
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
                 enable_adaptive_codec_selection = 0, default_compression_codec = 'LZ4',
                 enable_block_number_column = 0, enable_block_offset_column = 0;

        INSERT INTO t_merge_mem_subcolumn_codecs SELECT tuple(${element_values}) FROM numbers(100);
        INSERT INTO t_merge_mem_subcolumn_codecs SELECT tuple(${element_values}) FROM numbers(100, 100);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" --enable_subcolumn_codecs 1 -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_subcolumn_codecs MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

distinct_codecs=""
for i in $(seq 1 "$ELEMENTS")
do
    distinct_codecs+=", CODEC p.a${i} (ZSTD(${i}))"
done
with_distinct=$(reserved_for_merge_with_retries "$distinct_codecs")
without=$(reserved_for_merge_with_retries "")

echo "$((with_distinct > 0 && without > 0))"
# Nineteen more compressors of DBMS_DEFAULT_BUFFER_SIZE = 1048576 bytes.
echo "$((with_distinct - without >= (ELEMENTS - 1) * 1048576))"
