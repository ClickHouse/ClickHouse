#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO)

# The byte limit is checked between the blocks against the size of the data file written so far, so the file
# must be written in the inserting thread, also by the formats that would write it from threads of their own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROWBINARY_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_rowbinary"
PRETTY_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_pretty"

rm -rf "${ROWBINARY_PATH}" "${PRETTY_PATH}"

# Blocks of 100 rows from a single insert thread, so that the limit is checked between the blocks.
SETTINGS="
    SET allow_insert_into_iceberg = 1;
    SET max_insert_threads = 1, max_block_size = 100, max_insert_block_size = 100;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
    SET iceberg_insert_max_rows_in_data_file = 1000000000;
"

# A row format with parallel formatting; 1034 bytes per row, so every file but the last has 1000 rows.
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS}
    SET output_format_parallel_formatting = 1, max_threads = 8;
    SET iceberg_insert_max_bytes_in_data_file = 1000000;
    CREATE TABLE t_rowbinary (id Int64, s String) ENGINE = IcebergLocal('${ROWBINARY_PATH}', 'RowBinary');
    INSERT INTO t_rowbinary SELECT number, repeat('a', 1024) FROM numbers(4096);
    SELECT count(), arraySort(groupArray(record_count)) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't_rowbinary' AND content = 0;
    SELECT count(), sum(length(s)) FROM t_rowbinary;
"

# A Pretty format that would squash all blocks and write them after the INSERT; the default max_rows would stop the files growing.
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS}
    SET iceberg_insert_max_bytes_in_data_file = 100000;
    CREATE TABLE t_pretty (id Int64) ENGINE = IcebergLocal('${PRETTY_PATH}', 'PrettyCompact')
    SETTINGS output_format_pretty_squash_consecutive_ms = 60000, output_format_pretty_squash_max_wait_ms = 60000,
        output_format_pretty_max_rows = 100000;
    INSERT INTO t_pretty SELECT number FROM numbers(20000);
    SELECT max(file_size_in_bytes) < 200000, sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't_pretty' AND content = 0;
"

rm -rf "${ROWBINARY_PATH}" "${PRETTY_PATH}"
