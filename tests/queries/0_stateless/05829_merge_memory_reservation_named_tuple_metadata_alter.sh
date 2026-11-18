#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the failpoint used to park the merge is not available in the fast test build

# The merge memory reservation (`CompactionStatistics::estimateNeededMemoryForMerge`) must price the data that a
# type conversion synthesizes for the rows of parts written under an older type. With
# `allow_metadata_only_named_tuple_alter`, adding an element to a named `Tuple` does not rewrite the parts: the
# merge reads them, fills the new element with its type default and writes it for every row, although no source
# part stores a single byte of it. Here the new element is a `FixedString(8192)`, so the merge of 2000 rows writes
# about 16 MB that are not in the source parts' sizes, and the writer of that element grows its buffers to their
# full size.
#
# The reservation is read from the `MergesMutationsMemoryReservation` metric while the selected merge is parked
# on the `plain_merge_task_pause_before_prepare` failpoint (see `05220_merge_memory_reservation_marks_streams`).
# The same data is merged once without the `ALTER` and once after it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local alter="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05829_merge_memory_reservation_tuple_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SET allow_metadata_only_named_tuple_alter = 1, allow_suspicious_fixed_string_types = 1;
        CREATE TABLE t_merge_mem_tuple (k UInt64, t Tuple(x UInt8))
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 min_compress_block_size = 1048576, max_compress_block_size = 1048576,
                 min_columns_to_activate_adaptive_write_buffer = 1,
                 max_memory_ratio_to_activate_adaptive_write_buffer = 0,
                 vertical_merge_algorithm_min_rows_to_activate = 1000000000,
                 ratio_of_defaults_for_sparse_serialization = 1;

        INSERT INTO t_merge_mem_tuple SELECT number, tuple(1) FROM numbers(1000);
        INSERT INTO t_merge_mem_tuple SELECT number, tuple(1) FROM numbers(1000, 1000);
        ${alter}
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_tuple MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
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

without_alter=$(reserved_for_merge_with_retries "")
with_alter=$(reserved_for_merge_with_retries "ALTER TABLE t_merge_mem_tuple MODIFY COLUMN t Tuple(x UInt8, pad FixedString(8192));")

# Both merges were selected and reserved before they parked on the failpoint.
echo "$((without_alter > 0)) $((with_alter > 0))"
# The new stream writes 16 MB, so it fills its 1 MiB compressor and file buffer and allocates the 2 MiB compression
# scratch buffer. Without pricing the synthesized volume, only its small adaptive eager buffers were reserved.
echo "$((with_alter - without_alter >= 4194304))"
