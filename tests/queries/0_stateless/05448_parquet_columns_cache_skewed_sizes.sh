#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. The size of a chunk must
# not be underestimated when the values differ in size a lot, and estimating it must not read much. Each query below
# succeeds with the cache as it does without it:
# - a chunk to write to the cache, of dictionary-encoded strings without the length in the metadata, of which a long
#   value of the dictionary is repeated in most rows, while the other values are short;
# - a chunk served from the cache, wide, while the other chunks of the row group are narrow;
# - a column to write to the cache that `PREWHERE` does not need, with a large dictionary page;
# - a chunk to write to the cache that holds all the long strings, or all the non-null values, of the row group;
# - several columns to write to the cache that `PREWHERE` does not need, each with a dictionary page that is small,
#   but not all of them together.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
ENTRIES="SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

# Writes a `Parquet` file of dictionary-encoded strings without `size_statistics`, as older versions of PyArrow do.
function write_with_pyarrow()
{
    python3 - "${FILE_PATH}.tmp" "$1" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path, kind = sys.argv[1], sys.argv[2]
if kind == 'skewed_dictionary':
    # 65536 rows: each of 127 short values once, and a value of 2 KiB in all other rows.
    values = pa.array([str(i) for i in range(127)] + ['x' * 2048])
    indices = pa.array(list(range(128)) + [127] * (65536 - 128), type=pa.int32())
    table = pa.table({'s': pa.DictionaryArray.from_arrays(indices, values)})
    pq.write_table(table, path, row_group_size=1000000, use_dictionary=True)
elif kind == 'small_dictionaries':
    # 1536 rows: even numbers, and in each of six columns distinct values of 4 KiB in an uncompressed dictionary page
    # of 6 MiB.
    columns = {'id': pa.array([2 * i for i in range(1536)], type=pa.uint64())}
    for c in range(6):
        columns[f's{c}'] = pa.array([str(i + c).rjust(4096, 'x') for i in range(1536)])
    pq.write_table(pa.table(columns), path, row_group_size=1000000, use_dictionary=[f's{c}' for c in range(6)],
                   compression='NONE', dictionary_pagesize_limit=64 << 20)
else:
    # 12288 rows: even numbers, and distinct values of 4 KiB in an uncompressed dictionary page of 48 MiB.
    ids = pa.array([2 * i for i in range(12288)], type=pa.uint64())
    values = pa.array([str(i).rjust(4096, 'x') for i in range(12288)])
    pq.write_table(pa.table({'id': ids, 's': values}), path, row_group_size=1000000, use_dictionary=['s'],
                   compression='NONE', dictionary_pagesize_limit=64 << 20)
EOF
    python3 "${CUR_DIR}/helpers/parquet_strip_size_statistics.py" "${FILE_PATH}.tmp" "${FILE_PATH}"
    rm -f "${FILE_PATH}.tmp"
    # Only a file that has not been modified for a few seconds is cached.
    touch -d "1 minute ago" "${FILE_PATH}"
    $CLICKHOUSE_CLIENT -q "SELECT rg.columns.have_size_statistics FROM file('${FILE}', ParquetMetadata) ARRAY JOIN row_groups AS rg"
}

echo "--- dictionary-encoded strings without size statistics, mostly the longest value of the dictionary: not cached"
write_with_pyarrow skewed_dictionary
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi',
    max_memory_usage = '160Mi'"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

echo "--- a wide cached chunk among narrow ones"
# One row group of 16 chunks: the strings of the first chunk take 32 MiB, the others are empty.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT if(number < 65536, leftPad(toString(number % 16), 512, 'x'), '') AS s FROM numbers(1048576)
    SETTINGS output_format_parquet_row_group_size = 10000000, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String') SETTINGS max_threads = 1, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"
QUERY="SELECT sum(cityHash64(concat(s, '!'))) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '24Mi'"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

echo "--- a large dictionary page of a column that PREWHERE does not need"
write_with_pyarrow large_dictionary
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') PREWHERE id % 2 = 1
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi',
    max_memory_usage = '32Mi', use_query_condition_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

echo "--- several small dictionary pages of columns that PREWHERE does not need"
write_with_pyarrow small_dictionaries
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
QUERY="SELECT count(), sum(length(s0)), sum(length(s1)), sum(length(s2)), sum(length(s3)), sum(length(s4)), sum(length(s5))
    FROM file('${FILE}', Parquet, 'id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String') PREWHERE id % 2 = 1
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi',
    max_memory_usage = '32Mi', use_query_condition_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

# One row group of 16 chunks, read cold: the strings of the first chunk take 128 MiB, the others are empty or null.
function write_skewed_row_group()
{
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}.tmp', Parquet, 's $1')
        SELECT if(number < 65536, repeat('x', 2048), $2) FROM numbers(1048576)
        SETTINGS max_block_size = 1048576, output_format_parquet_row_group_size = 10000000, output_format_parquet_row_group_size_bytes = '1Gi',
        engine_file_truncate_on_insert = 1"
    if [ "$3" = strip ]; then
        python3 "${CUR_DIR}/helpers/parquet_strip_size_statistics.py" "${FILE_PATH}.tmp" "${FILE_PATH}"
        rm -f "${FILE_PATH}.tmp"
    else
        mv "${FILE_PATH}.tmp" "${FILE_PATH}"
    fi
    touch -d "1 minute ago" "${FILE_PATH}"
    $CLICKHOUSE_CLIENT -q "SELECT rg.columns.have_size_statistics FROM file('${FILE}', ParquetMetadata) ARRAY JOIN row_groups AS rg"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
    QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's $1')
        SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi',
        max_memory_usage = '160Mi'"
    $CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
    $CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"
    $CLICKHOUSE_CLIENT -q "${ENTRIES}"
}

echo "--- the long strings of a row group in one chunk, with size statistics"
write_skewed_row_group String "''" keep

echo "--- the non-null strings of a row group in one chunk, without size statistics"
write_skewed_row_group "Nullable(String)" NULL strip

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
