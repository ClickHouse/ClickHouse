#!/usr/bin/env bash
# Tags: no-fasttest

# `iceberg_insert_max_bytes_in_data_file` is compared against the size of the data file written so far,
# that is, the compressed size, the same measure as `file_size_in_bytes`, by which the compaction selects
# its input files. It used to be compared against the uncompressed size, which made the files smaller than
# the target by the compression ratio.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COMPRESSIBLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_compressible"
INCOMPRESSIBLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_incompressible"

rm -rf "${COMPRESSIBLE_PATH}" "${INCOMPRESSIBLE_PATH}"

SETTINGS="
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1, max_block_size = 100, max_insert_block_size = 100;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
    SET iceberg_insert_max_rows_in_data_file = 1000000000;
    SET iceberg_insert_max_bytes_in_data_file = '1Mi';
"

# The table takes its format settings from its `SETTINGS` clause, not from the session. Small row groups
# encoded in the writing thread, so that the Parquet writer writes the data to the file as it goes.
TABLE_SETTINGS="SETTINGS output_format_parquet_row_group_size = 100, output_format_parquet_parallel_encoding = 0,
    output_format_parquet_compression_method = 'zstd'"

# 4 MiB of uncompressed data that compresses to almost nothing: a single file.
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS}
    CREATE TABLE compressible (id Int64, s String)
    ENGINE = IcebergLocal('${COMPRESSIBLE_PATH}', 'Parquet') ${TABLE_SETTINGS};
    INSERT INTO compressible SELECT number, repeat('a', 1024) FROM numbers(4096);
"

# 4 MiB of incompressible data: several files, each about the limit.
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS}
    CREATE TABLE incompressible (id Int64, s String)
    ENGINE = IcebergLocal('${INCOMPRESSIBLE_PATH}', 'Parquet') ${TABLE_SETTINGS};
    INSERT INTO incompressible SELECT number, randomPrintableASCII(1024) FROM numbers(4096);
"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        count() > 1 AS multiple_files,
        sum(record_count) AS rows,
        countIf(file_size_in_bytes > 2 * 1024 * 1024) AS files_far_above_limit
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND content = 0
    GROUP BY table
    ORDER BY table
    FORMAT TSV;
"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(length(s)) FROM compressible;
    SELECT count(), sum(length(s)) FROM incompressible;
"

rm -rf "${COMPRESSIBLE_PATH}" "${INCOMPRESSIBLE_PATH}"
