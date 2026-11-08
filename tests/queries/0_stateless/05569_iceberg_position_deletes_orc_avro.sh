#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and ORC are not in fast test

# Position deletes on ORC and Avro data files (https://github.com/ClickHouse/ClickHouse/issues/130001).
# The tables are written by Spark with ORC / Avro data and delete files, see `data_iceberg/ice_lens/README.md`:
# 5 rows in 2 data files, position 1 of the first data file (`bravo`) is deleted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/ice_lens" || exit 1

for table in orcfmt avrofmt
do
    $CLICKHOUSE_LOCAL -q "SELECT '$table'; SELECT count() FROM icebergLocal('$table')"

    for bitmap in 0 1
    do
        # `max_block_size` and `input_format_orc_row_batch_size` = 1: one row per chunk, so every chunk needs its own row number.
        # `optimize_count_from_files` and `optimize_trivial_count_query` = 0: count the rows by reading them.
        $CLICKHOUSE_LOCAL -q "
            SELECT '$table', $bitmap;
            SELECT id, name FROM icebergLocal('$table') ORDER BY id SETTINGS use_roaring_bitmap_iceberg_positional_deletes = $bitmap;
            SELECT id, _row_number FROM icebergLocal('$table') ORDER BY id
                SETTINGS max_block_size = 1, input_format_orc_row_batch_size = 1, use_roaring_bitmap_iceberg_positional_deletes = $bitmap;
            SELECT id FROM icebergLocal('$table') WHERE id >= 2 ORDER BY id SETTINGS use_roaring_bitmap_iceberg_positional_deletes = $bitmap;
            SELECT count() FROM icebergLocal('$table')
                SETTINGS optimize_count_from_files = 0, optimize_trivial_count_query = 0, use_roaring_bitmap_iceberg_positional_deletes = $bitmap;
        "
    done
done
