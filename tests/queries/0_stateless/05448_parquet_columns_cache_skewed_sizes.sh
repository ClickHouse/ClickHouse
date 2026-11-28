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
# - several columns to write to the cache that `PREWHERE` does not need, each with a dictionary page that is small,
#   but not all of them together;
# - several columns to write to the cache that `PREWHERE` does not need, each with a small dictionary page followed by
#   large data pages, which are not read together with the dictionary page.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

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

echo "--- small dictionary pages of columns that PREWHERE does not need, each followed by large data pages"
# Each dictionary page fits the budget, but reading the data pages next to it, of 3 MiB each, as one read would not fit
# the memory limit. The large seek distance makes such a read likely.
write_with_pyarrow adjacent_data_pages
run_query "SELECT count(), sum(length(s0)), sum(length(s1)), sum(length(s2)), sum(length(s3)), sum(length(s4)), sum(length(s5))
    FROM file('${FILE}', Parquet, 'id UInt64, s0 String, s1 String, s2 String, s3 String, s4 String, s5 String') PREWHERE id % 2 = 1
    SETTINGS max_threads = 1, input_format_parquet_local_file_min_bytes_for_seek = 1048576, input_format_parquet_max_block_size = 1000,
    input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '8Mi',
    max_untracked_memory = 65536, max_read_buffer_size = 65536, use_query_condition_cache = 0"

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
