#!/usr/bin/env bash
# Tags: no-fasttest
# Reads Parquet files with `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY` and RLE / bit-packed
# encodings: the number of values decoded from a page is bounded by the number of values in the page.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for file in \
    04045_delta_sample_93093.parquet \
    05035_delta_byte_array_decimal.parquet \
    05318_parquet_rle_boolean.parquet \
    delta_length_byte_array_encoding.parquet \
    string_filters_delta_byte_array.parquet
do
    cp "$CUR_DIR/data_parquet/$file" "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_$file"
    echo "$file"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM file('${CLICKHOUSE_DATABASE}_$file', Parquet)"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM file('${CLICKHOUSE_DATABASE}_$file', Parquet) SETTINGS max_block_size = 3, input_format_parquet_max_block_size = 3"
    rm "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_$file"
done
