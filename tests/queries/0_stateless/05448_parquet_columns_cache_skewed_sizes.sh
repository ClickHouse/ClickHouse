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
#   value of the dictionary is repeated in most rows, while the other values are short, also without a declared
#   offset of the dictionary page, as old versions of DuckDB write;
# - a chunk served from the cache, wide, while the other chunks of the row group are narrow;
# - a column to write to the cache that `PREWHERE` does not need, with a large dictionary page;
# - a chunk to write to the cache that holds all the long strings, or all the non-null values, of the row group;
# - several columns to write to the cache that `PREWHERE` does not need, each with a dictionary page that is small,
#   but not all of them together;
# - a chunk to write to the cache, of strings encoded with `DELTA_BYTE_ARRAY` without the length in the metadata, which
#   take much less in the file than decoded;
# - a chunk to write to the cache, of short strings converted to a much wider type;
# - a chunk to write to the cache, with a wide column that is missing in the file and is filled with default values;
# - a chunk to write to the cache, with arrays of tuples of which all the requested elements are missing in the file;
# - a chunk to write to the cache, of a variant read as `JSON`, which takes much more memory than the binary columns of
#   the variant in the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
ENTRIES="SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

# Writes a `Parquet` file of dictionary-encoded strings without `size_statistics`, as older versions of PyArrow do.
# The optional second argument replaces `created_by` in the metadata.
function write_with_pyarrow()
{
    python3 - "${FILE_PATH}.tmp" "$1" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path, kind = sys.argv[1], sys.argv[2]
if kind == 'skewed_dictionary':
    # 65536 rows: each of 127 short values once, and a value of 512 bytes in all other rows.
    values = pa.array([str(i) for i in range(127)] + ['x' * 512])
    indices = pa.array(list(range(128)) + [127] * (65536 - 128), type=pa.int32())
    table = pa.table({'s': pa.DictionaryArray.from_arrays(indices, values)})
    pq.write_table(table, path, row_group_size=1000000, use_dictionary=True)
elif kind == 'small_dictionaries':
    # 384 rows: even numbers, and in each of six columns distinct values of 4 KiB in an uncompressed dictionary page
    # of 1.5 MiB.
    columns = {'id': pa.array([2 * i for i in range(384)], type=pa.uint64())}
    for c in range(6):
        columns[f's{c}'] = pa.array([str(i + c).rjust(4096, 'x') for i in range(384)])
    pq.write_table(pa.table(columns), path, row_group_size=1000000, use_dictionary=[f's{c}' for c in range(6)],
                   compression='NONE', dictionary_pagesize_limit=64 << 20)
elif kind == 'delta':
    # 65536 rows: the same value of 512 bytes, of which `DELTA_BYTE_ARRAY` stores only the length of the shared prefix.
    table = pa.table({'s': pa.array(['x' * 512] * 65536)})
    pq.write_table(table, path, row_group_size=1000000, use_dictionary=False, column_encoding={'s': 'DELTA_BYTE_ARRAY'},
                   compression='NONE')
elif kind == 'short_strings':
    # 65536 rows: strings of one byte.
    table = pa.table({'s': pa.array([str(i % 10) for i in range(65536)])})
    pq.write_table(table, path, row_group_size=1000000, use_dictionary=False, compression='NONE')
else:
    # 3072 rows: even numbers, and distinct values of 4 KiB in an uncompressed dictionary page of 12 MiB.
    ids = pa.array([2 * i for i in range(3072)], type=pa.uint64())
    values = pa.array([str(i).rjust(4096, 'x') for i in range(3072)])
    pq.write_table(pa.table({'id': ids, 's': values}), path, row_group_size=1000000, use_dictionary=['s'],
                   compression='NONE', dictionary_pagesize_limit=64 << 20)
EOF
    python3 "${CUR_DIR}/helpers/parquet_strip_size_statistics.py" "${FILE_PATH}.tmp" "${FILE_PATH}" ${2:+"$2"}
    rm -f "${FILE_PATH}.tmp"
    # Only a file that has not been modified for a few seconds is cached.
    touch -d "1 minute ago" "${FILE_PATH}"
}

# Runs the query without the columns cache and with it, from the empty cache, then shows what the cache holds.
function run_query()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT rg.columns.have_size_statistics FROM file('${FILE}', ParquetMetadata) ARRAY JOIN row_groups AS rg;
        SYSTEM DROP COLUMNS CACHE;
        $1, use_columns_cache = 0;
        $1, use_columns_cache = 1;
        ${ENTRIES}"
}

echo "--- dictionary-encoded strings without size statistics, mostly the longest value of the dictionary: not cached"
write_with_pyarrow skewed_dictionary
run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '40Mi'"

echo "--- the same, written by an old version of DuckDB, without dictionary_page_offset: not cached"
# For such files `dictionary_page_offset` is cleared, see `Reader::fixUpColumnChunk`, but the strings are still
# dictionary-encoded.
write_with_pyarrow skewed_dictionary DuckDB
run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '40Mi'"

echo "--- a wide cached chunk among narrow ones"
# One row group of 8 chunks: the strings of the first chunk take 16 MiB, the others are empty.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT if(number < 65536, leftPad(toString(number % 16), 256, 'x'), '') AS s FROM numbers(524288)
    SETTINGS output_format_parquet_row_group_size = 10000000, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
QUERY="SELECT sum(cityHash64(concat(s, '!'))) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '2Mi',
    max_memory_usage = '12Mi'"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String') SETTINGS max_threads = 1, use_columns_cache = 1;
    ${ENTRIES};
    ${QUERY}, use_columns_cache = 0;
    ${QUERY}, use_columns_cache = 1"

echo "--- a large dictionary page of a column that PREWHERE does not need"
write_with_pyarrow large_dictionary
run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') PREWHERE id % 2 = 1
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '8Mi', use_query_condition_cache = 0"

echo "--- several small dictionary pages of columns that PREWHERE does not need"
write_with_pyarrow small_dictionaries
run_query "SELECT count(), sum(length(s0)), sum(length(s1)), sum(length(s2)), sum(length(s3)), sum(length(s4)), sum(length(s5))
    FROM file('${FILE}', Parquet, 'id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String') PREWHERE id % 2 = 1
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '8Mi', use_query_condition_cache = 0"

echo "--- strings encoded with DELTA_BYTE_ARRAY without size statistics: not cached"
write_with_pyarrow delta
run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '40Mi'"

echo "--- short strings read as a wide FixedString: not cached"
write_with_pyarrow short_strings
run_query "SELECT count(), sum(cityHash64(s)) FROM file('${FILE}', Parquet, 's FixedString(256)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '12Mi'"

# One row group of 4 chunks, read cold: the strings of the first chunk take 32 MiB, the others are empty or null.
function write_skewed_row_group()
{
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}.tmp', Parquet, 's $1')
        SELECT if(number < 65536, repeat('x', 512), $2) FROM numbers(262144)
        SETTINGS max_block_size = 262144, output_format_parquet_row_group_size = 10000000, output_format_parquet_row_group_size_bytes = '1Gi',
        engine_file_truncate_on_insert = 1"
    if [ "$3" = strip ]; then
        python3 "${CUR_DIR}/helpers/parquet_strip_size_statistics.py" "${FILE_PATH}.tmp" "${FILE_PATH}"
        rm -f "${FILE_PATH}.tmp"
    else
        mv "${FILE_PATH}.tmp" "${FILE_PATH}"
    fi
    touch -d "1 minute ago" "${FILE_PATH}"
    run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's $1')
        SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
        max_memory_usage = '40Mi'"
}

echo "--- the long strings of a row group in one chunk, with size statistics"
write_skewed_row_group String "''" keep

echo "--- the non-null strings of a row group in one chunk, without size statistics"
write_skewed_row_group "Nullable(String)" NULL strip

echo "--- a wide column missing in the file: not cached"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT number AS id FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT sum(id), sum(cityHash64(missing)) FROM file('${FILE}', Parquet, 'id UInt64, missing FixedString(256)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '12Mi'"

echo "--- arrays of tuples of which all the requested elements are missing in the file: not cached"
# The arrays are empty, but their offsets take 8 bytes per row: 16 MiB in 32 columns.
ARRAYS=$(seq -s ', ' -f 'a%g' 0 31)
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'id UInt64, $(seq -s ', ' -f 'a%g Array(Tuple(old UInt8))' 0 31)')
    SELECT number$(printf ', []%.0s' $(seq 32)) FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT sum(id), sum(cityHash64(tuple(${ARRAYS})))
    FROM file('${FILE}', Parquet, 'id UInt64, $(seq -s ', ' -f 'a%g Array(Tuple(missing UInt8))' 0 31)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '12Mi'"

echo "--- a variant read as JSON: not cached"
# 65536 rows of an unshredded variant, recognized by its structure: objects of one field, of 64 distinct names, with
# a value of type `Int8`. Each name is a path of `JSON`, which takes a value in every row.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'v Tuple(metadata String, value String)')
    SELECT tuple(concat(unhex('01010005'), 'k', leftPad(toString(number % 64), 4, '0')), unhex('02010000020C05')) FROM numbers(65536)
    SETTINGS output_format_parquet_string_as_string = 0, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT count(), sum(length(toJSONString(v))) FROM file('${FILE}', Parquet, 'v JSON')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '256Ki', max_memory_usage = '24Mi'"

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
