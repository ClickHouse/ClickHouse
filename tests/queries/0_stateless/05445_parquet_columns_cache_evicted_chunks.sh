#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the sizes of the chunks depend on the settings

# A column of a `Parquet` row group is served from the columns cache only with all its chunks of 65536 rows. When the
# column does not fit into the cache while each of its chunks does, the chunks written later evict the earlier ones,
# and the chunks that are left are of no use: they are taken back rather than kept.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# One row group of 32 chunks. The strings are dictionary-encoded, so the file is small, but decoded, the column `s`
# takes about 146 MiB, more than the cache of 100 MiB, and a chunk about 5 MiB.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT leftPad(toString(number % 16), 64, 'x') AS s FROM numbers(2097152)
    SETTINGS output_format_parquet_row_group_size = 10000000, engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "SELECT num_row_groups, num_rows FROM file('${FILE}', ParquetMetadata)"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

for _ in 1 2
do
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String') SETTINGS max_threads = 1, use_columns_cache = 1"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')"
done

rm -f "${FILE_PATH}"
