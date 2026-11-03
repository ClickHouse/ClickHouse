#!/usr/bin/env bash
# Tags: no-fasttest

# Bit-packed dictionary indexes of every width from 1 to 18 bits and definition/repetition levels of widths 1 and 2
# decode to the values that were written, also when a block starts or ends inside a bit-packed run.
# A definition level out of range in a bit-packed run is rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

F="${CLICKHOUSE_DATABASE}_w.parquet"
F_OK="${CLICKHOUSE_DATABASE}_lv_ok.parquet"
F_BAD="${CLICKHOUSE_DATABASE}_lv_bad.parquet"
trap 'rm -f "${USER_FILES_PATH:?}/${F}" "${USER_FILES_PATH:?}/${F_OK}" "${USER_FILES_PATH:?}/${F_BAD}"' EXIT

# xk has 2^k distinct values (an odd multiplier is a bijection modulo 2^k), so its dictionary indexes are k bits wide.
# The first 300 rows of each 1000 take at most 5 distinct values, which gives RLE runs.
COLS="number AS n"
for k in $(seq 1 18); do
    COLS="$COLS,
    toUInt32(if(number % 1000 < 300, intDiv(number, 97) % $(( (1 << k) < 5 ? (1 << k) : 5 )), (number * $((2654435761 + 2 * k))) % $((1 << k)))) AS x$k"
done

${CLICKHOUSE_CLIENT} --query "
CREATE VIEW src AS SELECT $COLS,
    toString(intHash32(number + 100) % 8192) AS s,
    if(number % 13 = 0, NULL, toUInt16(intHash32(number + 200) % 2048)) AS y,
    arrayMap(i -> toUInt32(intHash32(number * 7 + i) % 512), range(number % 4)) AS z,
    arrayMap(i -> if((number + i) % 5 = 0, NULL, toUInt32(intHash32(number * 3 + i) % 64)), range(number % 3)) AS w
FROM numbers(200000)"

# Small pages, so the pages of each column go through the smaller widths while the dictionary grows.
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION file('${F}') SELECT * FROM src
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000,
    output_format_parquet_data_page_size = 4096, output_format_parquet_max_dictionary_size = 1048576"

${CLICKHOUSE_CLIENT} --query "
SELECT num_row_groups,
    arrayMap(c -> c.name, arrayFilter(c -> NOT has(c.encodings, 'RLE_DICTIONARY'), columns)),
    arrayMap(c -> (c.name, c.max_definition_level, c.max_repetition_level), arrayFilter(c -> c.max_definition_level > 0, columns))
FROM file('${F}', ParquetMetadata)"

WIDTHS=""
AGG="count()"
STRUCTURE="n UInt64"
for k in $(seq 1 18); do
    WIDTHS="$WIDTHS ceil(log2(uniqExact(x$k))),"
    AGG="$AGG, sum(cityHash64(n, x$k))"
    STRUCTURE="$STRUCTURE, x$k UInt32"
done
STRUCTURE="$STRUCTURE, s String, y Nullable(UInt16), z Array(UInt32), w Array(Nullable(UInt32))"
AGG="$AGG, sum(cityHash64(n, s)), sum(cityHash64(n, y)), sum(cityHash64(n, z)), sum(cityHash64(n, w))"
${CLICKHOUSE_CLIENT} --query "SELECT $WIDTHS ceil(log2(uniqExact(s))) FROM src"

FILE="file('${F}', Parquet, '${STRUCTURE}')"
# One row of booleans: the row count, then each column's hash sum, read from the file vs computed from `src`.
compare()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT arrayMap((a, b) -> a = b, (SELECT [$AGG] FROM $FILE $1), (SELECT [$AGG] FROM src $2))"
}
compare "" ""
# Blocks of 777 rows start and end inside bit-packed runs, at bit positions that are not byte aligned.
compare "SETTINGS input_format_parquet_max_block_size = 777" ""
compare "PREWHERE x5 % 3 = 1" "WHERE x5 % 3 = 1"

# Definition levels 1, 2, 2, ... (max 2) are bit-packed at width 2 as bytes 69 9A A6; changing the first 69 to 6B makes
# the first level 3.
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION file('${F_OK}') SELECT [if(number % 3 = 0, NULL, toUInt8(1))] AS a FROM numbers(3000)
SETTINGS output_format_parquet_compression_method = 'none', output_format_parquet_write_checksums = 0, engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "SELECT position(x, unhex('699AA6699AA6699AA6')) > 0 FROM file('${F_OK}', 'RawBLOB', 'x String')"
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION file('${F_BAD}', 'RawBLOB') SELECT overlay(x, '\x6B', position(x, unhex('699AA6699AA6699AA6')))
FROM file('${F_OK}', 'RawBLOB', 'x String') SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a[1]) FROM file('${F_OK}')"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a[1]) FROM file('${F_BAD}') SETTINGS input_format_parquet_verify_checksums = 0" 2>&1 \
    | grep -o -m1 'Dict index or rep/def level out of bounds (bp)'
