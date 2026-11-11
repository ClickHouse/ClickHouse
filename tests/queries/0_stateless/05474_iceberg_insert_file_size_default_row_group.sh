#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# `iceberg_insert_max_bytes_in_data_file` is compared against the bytes written to the data file so far, and the Parquet
# writer writes a row group only when it is complete. With the default row group size (512 MiB before compression), a
# file smaller than a row group was never cut, so the limit was ignored. The row groups are now not bigger than the limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_default_row_group"

rm -rf "${TABLE_PATH}"

# 16 MiB of incompressible data with a limit of 1 MiB and the default Parquet settings of the table. The limit is checked
# between the blocks, so they are small.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1, max_block_size = 100, max_insert_block_size = 100;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
    SET iceberg_insert_max_rows_in_data_file = 1000000000;
    SET iceberg_insert_max_bytes_in_data_file = '1Mi';
    CREATE TABLE t (id Int64, s String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t SELECT number, randomPrintableASCII(1024) FROM numbers(16384);
"

${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 1 AS multiple_files, sum(record_count) AS rows
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't' AND content = 0
    FORMAT TSV;
"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)) FROM t"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

rm -rf "${TABLE_PATH}"
