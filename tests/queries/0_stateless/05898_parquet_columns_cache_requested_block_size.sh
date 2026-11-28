#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks that it is hit
# no-parallel-replicas: profile events are not available on the second replica
# no-random-settings: the block sizes depend on the block size settings and on the cache settings

# A `Parquet` row group served from the columns cache is read in the blocks the settings ask for, not in
# whole chunks of the cache: the rows of this file are wide, so `input_format_parquet_prefer_block_bytes`
# asks for about 1250 of them, and the whole file of 50000 rows is a single chunk of the cache. A single
# block would be processed by a single thread.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, arrayMap(x -> number * 100 + x, range(100)) AS a FROM numbers(50000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d '1 minute ago' "${FILE_PATH}"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

SETTINGS="use_columns_cache = 1, input_format_parquet_prefer_block_bytes = 1000000"

# The first read fills the cache with whole chunks, the second one is served from it.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_first" -q "
    SELECT sum(cityHash64(a)) FROM file('${FILE}') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_second" -q "
    SELECT max(blockSize()) <= 16384, sum(cityHash64(a)) FROM file('${FILE}') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "
    SELECT max(blockSize()) <= 16384, sum(cityHash64(a)) FROM file('${FILE}') SETTINGS use_columns_cache = 0, input_format_parquet_prefer_block_bytes = 1000000"

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['ColumnsCacheHits'] > 0, ProfileEvents['ColumnsCacheMisses']
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_second' AND type = 'QueryFinish' AND current_database = currentDatabase()"

rm -f "${FILE_PATH}"
