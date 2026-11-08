#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and ORC are not in fast test

# Position deletes on ORC and Avro data files (https://github.com/ClickHouse/ClickHouse/issues/130001).
# The tables are written by Spark with ORC / Avro data and delete files, see `data_iceberg/ice_lens/README.md`:
# 5 rows in 2 data files, position 1 of the first data file (`bravo`) is deleted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(realpath "$(mktemp -d -p "$CLICKHOUSE_TMP")")
trap 'rm -rf "$TMP_DIR"' EXIT

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

# Compaction must refuse these tables: after a rename it would rewrite the renamed column with NULLs and remove the original files.
# The cloud build routes `OPTIMIZE` through a different code path, so only the surviving data is checked there.
IS_CLOUD=$($CLICKHOUSE_LOCAL -q "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
cp -r orcfmt avrofmt "$TMP_DIR/"
cd "$TMP_DIR" || exit 1
for table in orcfmt avrofmt
do
    $CLICKHOUSE_LOCAL --path "local_$table" -q "
        CREATE TABLE t ENGINE = IcebergLocal('$table');
        ALTER TABLE t RENAME COLUMN name TO renamed_name SETTINGS allow_insert_into_iceberg = 1;
    "
    OPTIMIZE_ERR=$($CLICKHOUSE_LOCAL --path "local_$table" -q "OPTIMIZE TABLE t SETTINGS allow_experimental_iceberg_compaction = 1" 2>&1)
    if [[ "$IS_CLOUD" = "1" ]] || grep -qF 'NOT_IMPLEMENTED' <<< "$OPTIMIZE_ERR"; then
        echo "$table OPTIMIZE refused"
    else
        echo "$table FAIL: $OPTIMIZE_ERR"
    fi
    $CLICKHOUSE_LOCAL --path "local_$table" -q "SELECT id, renamed_name FROM t ORDER BY id"
done
