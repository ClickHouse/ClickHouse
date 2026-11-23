#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: pyarrow

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DIR"
trap 'rm -rf "$DIR"' EXIT

# Nulls, empty arrays, nested types, a column of nulls only (pages without values), and rows longer than a page.
SOURCE="
SELECT
    number AS id,
    if(number % 3 = 0, NULL, toInt64(number) * 7 - 5000) AS n,
    toString(number % 100) AS s,
    arrayMap(i -> if((number + i) % 5 = 0, NULL, toInt32(i)), range(number % 7)) AS arr,
    arrayMap(i -> arrayMap(j -> toString(i * j), range(i % 3)), range(number % 4)) AS nested,
    mapFromArrays(arrayMap(i -> toString(i), range(number % 3)), range(number % 3)) AS m,
    CAST((if(number % 2 = 0, NULL, number / 3), range(number % 5)), 'Tuple(a Nullable(Float64), b Array(UInt8))') AS t,
    number % 3 = 1 AS b,
    toLowCardinality(if(number % 6 = 0, NULL, toString(number % 10))) AS lc,
    toFixedString(leftPad(toString(number % 10000), 4, '0'), 4) AS fs,
    CAST(NULL, 'Nullable(Int32)') AS all_null,
    if(number % 1000 < 2, range(3000), []) AS long_arr
FROM numbers(30000)"

SETTINGS="engine_file_truncate_on_insert = 1, output_format_parquet_use_data_page_v2 = 1,
    output_format_parquet_row_group_size = 10000, output_format_parquet_data_page_size = 4096"

expected=$($CLICKHOUSE_LOCAL -q "$SOURCE ORDER BY id FORMAT TSV" | md5sum)

write_and_check()
{
    $CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$DIR/$1.parquet') $SOURCE SETTINGS $SETTINGS, $2"
    actual=$($CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DIR/$1.parquet') ORDER BY id FORMAT TSV" | md5sum)
    if [ "$actual" = "$expected" ]; then echo "$1: OK"; else echo "$1: MISMATCH"; fi
}

for codec in none lz4 zstd snappy gzip brotli; do
    write_and_check "$codec" "output_format_parquet_compression_method = '$codec'"
done
# Without the page index the reader skips pages by the row counts in their headers.
write_and_check plain "output_format_parquet_max_dictionary_size = 0, output_format_parquet_write_page_index = 0"

for f in zstd plain; do
    $CLICKHOUSE_LOCAL -q "SELECT count(), sum(length(arr)), sum(length(long_arr)), sum(n) FROM file('$DIR/$f.parquet') PREWHERE id BETWEEN 12345 AND 13456"
done
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(length(arr)), sum(length(long_arr)), sum(n) FROM ($SOURCE) WHERE id BETWEEN 12345 AND 13456"

# Values that the compression doesn't shrink are stored uncompressed, so nothing is decompressed on read.
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/random.parquet') SELECT cityHash64(number) AS h FROM numbers(100000)
    SETTINGS $SETTINGS, output_format_parquet_compression_method = 'zstd', output_format_parquet_max_dictionary_size = 0;
    SELECT sum(h) = (SELECT sum(cityHash64(number)) FROM numbers(100000)) FROM file('$DIR/random.parquet');
    SELECT sum(value) FROM system.events WHERE event = 'ParquetDecompressedBytes'"

$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$DIR/v1.parquet') $SOURCE SETTINGS $SETTINGS, output_format_parquet_use_data_page_v2 = 0"

# The pages are `DATA_PAGE_V2` (a page header starts with its type: 0x15, then zigzag(3)), and pyarrow reads them as V1.
python3 - "$DIR" <<'PY'
import sys
import pyarrow.parquet as pq

d = sys.argv[1]
v1 = pq.read_table(f"{d}/v1.parquet")
for name in ["none", "lz4", "zstd", "snappy", "gzip", "brotli", "plain"]:
    path = f"{d}/{name}.parquet"
    meta = pq.ParquetFile(path).metadata
    data = open(path, "rb").read()
    chunks = [meta.row_group(r).column(c) for r in range(meta.num_row_groups) for c in range(meta.num_columns)]
    is_v2 = all(data[c.data_page_offset:c.data_page_offset + 2] == b"\x15\x06" for c in chunks)
    print(name, is_v2, pq.read_table(path).equals(v1))
PY
