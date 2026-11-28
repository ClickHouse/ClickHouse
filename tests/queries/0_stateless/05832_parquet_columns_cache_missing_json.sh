#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. A `JSON` column missing
# in the file is filled with compact default values, but their output is not bounded by their size: `toJSONString`
# repeats the name of a typed path in every row. So such a chunk is of unknown size, and is not read whole, also when
# the `JSON` is inside a missing `Tuple`. Each query succeeds with the cache as it does without it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT number AS id FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"

KEY=$(printf 'k%.0s' $(seq 1024))
SETTINGS="max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_allow_missing_columns = 1,
    input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '48Mi', max_untracked_memory = 65536"

echo "--- a JSON column missing in the file: not cached"
run_query "SELECT count(), sum(id), sum(length(toJSONString(missing)))
    FROM file('${FILE}', Parquet, 'id UInt64, missing JSON(${KEY} Int8)') SETTINGS ${SETTINGS}"

echo "--- a tuple with a JSON element missing in the file: not cached"
run_query "SELECT count(), sum(id), sum(length(toJSONString(missing.j)))
    FROM file('${FILE}', Parquet, 'id UInt64, missing Tuple(x UInt8, j JSON(${KEY} Int8))') SETTINGS ${SETTINGS}"

rm -f "${FILE_PATH}"
