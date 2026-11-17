#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: DataSketches is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$OUT"
rm -f "${OUT:?}"/*.puffin

THETA="output_format_puffin_blob_type = 'apache-datasketches-theta-v1', output_format_puffin_snapshot_id = 123, output_format_puffin_sequence_number = 7"

echo "--- one blob per column, exact ndv for small inputs ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/types.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20'
SELECT
    number % 2 = 0 AS b,
    toInt8(number % 100) AS i8,
    toUInt16(number % 300) AS u16,
    toInt32(number) - 500 AS i32,
    toUInt32(number % 400) AS u32,
    toInt64(number % 600) AS i64,
    toFloat32(number % 50) / 4 AS f32,
    toFloat64(number % 70) / 3 AS f64,
    toDate('2020-01-01') + number % 30 AS d,
    toDate32('1950-01-01') + number % 40 AS d32,
    toDateTime('2020-01-01 00:00:00', 'UTC') + number % 90 AS dt,
    toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + toIntervalMillisecond(number % 110) AS dt64,
    toString(number % 120) AS s,
    toFixedString(toString(number % 10), 2) AS fs,
    toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number % 130), 12, '0'))) AS uuid,
    toDecimal32(number % 140, 2) AS dec32,
    toDecimal256(number % 150, 20) AS dec256,
    if(number % 3 = 0, NULL, number % 160) AS nullable,
    toLowCardinality(toString(number % 170)) AS lc,
    toLowCardinality(if(number % 4 = 0, NULL, toString(number % 180))) AS lc_nullable
FROM numbers(1000)
"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, snapshot_id, sequence_number, fields, compression_codec, properties
FROM file('$OUT/types.puffin', PuffinMetadata) ORDER BY offset
"

echo "--- NULLs and empty strings are skipped, empty inputs give empty sketches ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/skipped.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1,2'
SELECT if(number % 2 = 0, '', toString(number)) AS s, CAST(NULL AS Nullable(Int32)) AS n FROM numbers(10)
"
$CLICKHOUSE_LOCAL -q "SELECT fields, properties['ndv'] FROM file('$OUT/skipped.puffin', PuffinMetadata) ORDER BY offset"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/empty.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1'
SELECT number AS x FROM numbers(0)
"
$CLICKHOUSE_LOCAL -q "SELECT fields, length, properties['ndv'] FROM file('$OUT/empty.puffin', PuffinMetadata)"

echo "--- the same values give the same sketch for all integer types of Iceberg int, and another one for long ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/widths.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1,2,3,4'
SELECT toInt8(number) AS a, toUInt16(number) AS b, toInt32(number) AS c, toInt64(number) AS d FROM numbers(100)
"
$CLICKHOUSE_LOCAL -q "
SELECT arraySort(groupArray(m.fields[1]))
FROM file('$OUT/widths.puffin', PuffinMetadata) AS m, file('$OUT/widths.puffin', RawBLOB, 'raw String') AS r
GROUP BY substring(r.raw, m.offset + 1, m.length)
ORDER BY 1
"

echo "--- estimation mode ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/large.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1'
SELECT number % 100000 AS x FROM numbers(300000)
"
$CLICKHOUSE_LOCAL -q "SELECT abs(toUInt64(properties['ndv']) - 100000) < 5000 FROM file('$OUT/large.puffin', PuffinMetadata)"

echo "--- field ids ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/field_ids.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = ' 5, 3 '
SELECT number AS a, toString(number) AS b FROM numbers(3)
"
$CLICKHOUSE_LOCAL -q "SELECT fields, properties['ndv'] FROM file('$OUT/field_ids.puffin', PuffinMetadata) ORDER BY offset"

echo "--- reading: Puffin skips Theta sketch blobs ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$OUT/field_ids.puffin', Puffin)"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$OUT/field_ids.puffin', Puffin, 'referenced_data_file String')"

echo "--- reading: blobs of unknown types are listed by PuffinMetadata and skipped by Puffin ---"
$CLICKHOUSE_LOCAL -q "
WITH '{\"blobs\":[{\"type\":\"some-future-blob\",\"fields\":[1],\"snapshot-id\":1,\"sequence-number\":2,\"offset\":4,\"length\":3,\"compression-codec\":\"zstd\"}]}' AS footer
SELECT concat('PFA1', 'abc', 'PFA1', footer, reinterpretAsFixedString(toInt32(length(footer))), repeat('\\0', 4), 'PFA1')
INTO OUTFILE '$OUT/unknown.puffin' FORMAT RawBLOB
"
$CLICKHOUSE_LOCAL -q "SELECT blob_type, fields, offset, length, compression_codec, properties FROM file('$OUT/unknown.puffin', PuffinMetadata)"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$OUT/unknown.puffin', Puffin)"

echo "--- errors ---"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e1.puffin', Puffin) SETTINGS output_format_puffin_blob_type = 'unknown' SELECT 1 AS x" 2>&1 | grep -o -m1 "Unknown Puffin blob type 'unknown'"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e2.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1' SELECT 1 AS x, 2 AS y" 2>&1 | grep -o -m1 "has 1 field ids, but 2 columns are written"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e3.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1,1' SELECT 1 AS x, 2 AS y" 2>&1 | grep -o -m1 "contains field id 1 twice"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e4.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1,x' SELECT 1 AS x, 2 AS y" 2>&1 | grep -o -m1 "must be a comma-separated list of positive integers"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e5.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1' SELECT [1] AS x" 2>&1 | grep -o -m1 "the type has no Iceberg single-value serialization"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e6.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1' SELECT now64(9) AS x" 2>&1 | grep -o -m1 "cast the column to DateTime64(6)"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e7.puffin', Puffin) SETTINGS $THETA SELECT 1 AS x" 2>&1 | grep -o -m1 "Setting \`output_format_puffin_field_ids\` must be set"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e8.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1', output_format_puffin_snapshot_id = -1 SELECT 1 AS x" 2>&1 | grep -o -m1 "Setting \`output_format_puffin_snapshot_id\` must be set"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$OUT/e9.puffin', Puffin) SETTINGS $THETA, output_format_puffin_field_ids = '1', output_format_puffin_sequence_number = -1 SELECT 1 AS x" 2>&1 | grep -o -m1 "Setting \`output_format_puffin_sequence_number\` must be set"

rm -rf "${OUT:?}"
