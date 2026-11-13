#!/usr/bin/env bash

# Reads Compact parts whose columns share compressed blocks (`compress_per_column_in_compact_parts = 0`), so the marks
# of all columns but the first point inside a block, from object storage with a read buffer per column.
# The read of a column must not stop at the start of the block that holds the end of its data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings=(
    --max_threads=1
    # A buffer per column for parts with stripes of any size.
    --merge_tree_compact_parts_min_granules_to_multibuffer_read=1
    # With the `read` method a part on a local object storage is read with a single buffer.
    --local_filesystem_read_method=pread
)

# Marks for substreams or for columns, one or several granules in a stripe.
for substream_marks in 1 0; do
    for granules_in_stripe in 1 3; do
        table="t_shared_block_${substream_marks}_${granules_in_stripe}"
        $CLICKHOUSE_CLIENT "${settings[@]}" -q "
            DROP TABLE IF EXISTS ${table};

            CREATE TABLE ${table} (key UInt64, n Nullable(UInt64), a Array(UInt64), u UInt64)
            ENGINE = MergeTree ORDER BY key
            SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${table}/'),
                min_bytes_for_wide_part = '10G', index_granularity = 8, index_granularity_bytes = '10M',
                compress_per_column_in_compact_parts = 0,
                compact_parts_max_granules_to_buffer = ${granules_in_stripe},
                compact_parts_max_bytes_to_buffer = '128M',
                write_marks_for_substreams_in_compact_parts = ${substream_marks};

            INSERT INTO ${table} SELECT number, if(number % 3 = 0, NULL, number), range(number % 4), number * 2 FROM numbers(80);

            SELECT '${table}', part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active;

            -- Each column alone, a range that starts in the middle of the part, and all columns together.
            SELECT 'key', sum(key) FROM ${table};
            SELECT 'n', sum(n), countIf(n IS NULL) FROM ${table};
            SELECT 'a', sum(length(a)), sum(arraySum(a)) FROM ${table};
            SELECT 'u', sum(u) FROM ${table};
            SELECT 'range', count(), sum(key), sum(n), sum(length(a)), sum(u) FROM ${table} WHERE key >= 20;
            SELECT 'all', count(), sum(cityHash64(key, n, a, u)) FROM ${table};

            DROP TABLE ${table} SYNC;
        "
    done
done
