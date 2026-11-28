#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. An `Enum` takes only an
# integer per value, but its names, which a query may take for each value, e.g. by `toString`, may be long. The size of
# a chunk must count them, both of a chunk to write to the cache and of a chunk served from the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

# 65536 rows of the same value of `Int8`, read as an `Enum8` with a name of 1 KiB: 64 MiB as strings.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'e Int8') SELECT 1 FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"

NAME=$(printf 'x%.0s' {1..1024})
STRUCTURE="'e Enum8(\\'${NAME}\\' = 1)'"
SETTINGS="max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_max_block_size = 1000,
    input_format_parquet_prefer_block_bytes = 0, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '48Mi', max_untracked_memory = 65536"
QUERY="SELECT count(), sum(cityHash64(toString(e))) FROM file('${FILE}', Parquet, ${STRUCTURE}) SETTINGS ${SETTINGS}"

echo "--- a chunk to write to the cache: not cached"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    ${QUERY}, use_columns_cache = 0;
    ${QUERY}, use_columns_cache = 1;
    ${ENTRIES}"

echo "--- a chunk served from the cache"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    SELECT count(), sum(toInt8(e)) FROM file('${FILE}', Parquet, ${STRUCTURE}) SETTINGS max_threads = 1, use_columns_cache = 1;
    ${ENTRIES};
    ${QUERY}, use_columns_cache = 1"

# A missing `Tuple` of 16 `Enum8` with names of 64 bytes, filled with defaults: 16 names per row, 70 MiB as strings.
# It has no primitive columns to charge the names to, so the size of a value must count all of them.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'id UInt64') SELECT number FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"

NAME=$(printf 'x%.0s' {1..64})
ELEMENTS="e0 Enum8(\\'${NAME}\\' = 0)"
for i in {1..15}; do ELEMENTS="${ELEMENTS}, e${i} Enum8(\\'${NAME}\\' = 0)"; done
STRUCTURE="'id UInt64, missing Tuple(${ELEMENTS})'"
QUERY="SELECT count(), sum(id), sum(cityHash64(toString(missing))) FROM file('${FILE}', Parquet, ${STRUCTURE})
    SETTINGS ${SETTINGS}, input_format_parquet_allow_missing_columns = 1"

echo "--- a missing tuple of enums"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    ${QUERY}, use_columns_cache = 0;
    ${QUERY}, use_columns_cache = 1"

rm -f "${FILE_PATH}"
