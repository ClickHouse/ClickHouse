#!/usr/bin/env bash
# Tags: no-fasttest
# Dictionary-encoded columns read under a PREWHERE that keeps part of the rows return the source values, for filters of
# several densities and with byte values above 1, and for RLE and bit-packed runs shorter and longer than 64 values.
# The file has no page index, so the filter is applied while decoding the dictionary indexes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
# x: RLE runs of 97 and long bit-packed runs; y: alternating ~40-value bit-packed and ~90-value RLE runs.
SRC="SELECT number AS id,
        toUInt32(if(number % 1000 < 300, intDiv(number, 97) % 5, intHash32(number) % 1024)) AS x,
        toUInt32(if(number % 130 < 40, intHash32(number * 7) % 1024, intDiv(number, 130) % 1024)) AS y,
        toString(intHash32(number * 3) % 1024) AS s,
        toUInt16(intDiv(number, 50) % 7) AS r
    FROM numbers(150000)"
AGG="count(), sum(cityHash64(id, x)), sum(cityHash64(id, y)), sum(cityHash64(id, s)), sum(cityHash64(id, r))"
SETTINGS="input_format_parquet_prefer_block_bytes = 0, input_format_parquet_filter_push_down = 0,
    input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0,
    use_query_condition_cache = 0"

Q="INSERT INTO FUNCTION file('$FILE') $SRC SETTINGS output_format_parquet_write_page_index = 0,
    output_format_parquet_row_group_size = 70000, output_format_parquet_data_page_size = 4096,
    engine_file_truncate_on_insert = 1;
SELECT tupleElement(c, 'name'), has(tupleElement(c, 'encodings'), 'RLE_DICTIONARY')
FROM file('$FILE', ParquetMetadata) ARRAY JOIN columns AS c WHERE tupleElement(c, 'name') != 'id';"
for c in "id % 3 = 0" "id % 64 < 5" "intHash32(id) % 100 < 2" "id % 1000 < 999" "id BETWEEN 12345 AND 12399" "toUInt8(id % 5)"; do
    Q="$Q
SELECT '$c',
    (SELECT ($AGG) FROM file('$FILE') PREWHERE $c SETTINGS $SETTINGS, input_format_parquet_max_block_size = 1000)
        = (SELECT ($AGG) FROM ($SRC) WHERE $c),
    (SELECT ($AGG) FROM file('$FILE') PREWHERE $c SETTINGS $SETTINGS, max_threads = 1)
        = (SELECT ($AGG) FROM ($SRC) WHERE $c);"
done
$CLICKHOUSE_LOCAL -q "$Q"

rm -f "$FILE"
