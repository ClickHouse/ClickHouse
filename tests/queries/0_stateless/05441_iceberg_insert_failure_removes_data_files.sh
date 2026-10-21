#!/usr/bin/env bash
# Tags: no-fasttest
# A failed insert into Iceberg must remove the data files it already uploaded, and keep the committed ones.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_failed_insert"
rm -rf "${TABLE_PATH}"

# One row per data file, so the failing insert has uploaded several files before the exception.
ONE_ROW_PER_FILE="
    SET allow_experimental_insert_into_iceberg = 1;
    SET iceberg_insert_max_rows_in_data_file = 1;
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 1, max_insert_block_size = 1;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
"

${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE t (id UInt64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t SELECT number FROM numbers(3);
"

${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    INSERT INTO t SELECT throwIf(number = 20, 'expected') + number FROM numbers(30);
" 2>&1 | grep -c -m1 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

echo '--- rows ---'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t"

echo '--- data files on disk ---'
find "${TABLE_PATH}/data" -type f -name '*.parquet' | wc -l

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
rm -rf "${TABLE_PATH}"
