#!/usr/bin/env bash
# Tags: no-fasttest
# A failed insert into Iceberg must remove the data files it already uploaded, and keep the committed ones,
# both when the exception reaches the sink and when the pipeline is cancelled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_aborted_insert"
BAD_CSV="${CLICKHOUSE_DATABASE}_aborted_insert.csv"
rm -rf "${TABLE_PATH}"

# 20 valid rows, then a row that cannot be parsed as Int64.
{ seq 0 19; echo 'x'; } > "${CLICKHOUSE_USER_FILES}/${BAD_CSV}"

# One row per data file, so the failing insert has uploaded several files before the exception.
ONE_ROW_PER_FILE="
    SET allow_experimental_insert_into_iceberg = 1;
    SET iceberg_insert_max_rows_in_data_file = 1;
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 1, max_insert_block_size = 1;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
"

${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE t (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t SELECT number FROM numbers(3);
"

${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    INSERT INTO t SELECT throwIf(number = 20, 'expected') + number FROM numbers(30);
" 2>&1 | grep -c -m1 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

# The exception is thrown by the source, so the pipeline is cancelled and the sink's `onException` is not called.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    INSERT INTO t SELECT id FROM file('${BAD_CSV}', 'CSV', 'id Int64') SETTINGS input_format_parallel_parsing = 0;
" 2>&1 | grep -c -m1 'CANNOT_PARSE'

echo '--- rows ---'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t"

echo '--- data files on disk ---'
find "${TABLE_PATH}/data" -type f -name '*.parquet' | wc -l

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
rm -rf "${TABLE_PATH}" "${CLICKHOUSE_USER_FILES}/${BAD_CSV}"
