#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: DataSketches is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reference is printed by data_puffin/generate_theta_reference_hashes.py, which encodes the same values
# with the Iceberg single-value serialization and hashes them as Apache DataSketches does.

OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$OUT"

$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/reference.puffin', Puffin) SETTINGS output_format_puffin_blob_type = 'apache-datasketches-theta-v1', output_format_puffin_field_ids = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15', output_format_puffin_snapshot_id = 1, output_format_puffin_sequence_number = 1
SELECT
    (n = 0)::Bool AS b,
    toInt8([-128, 5][n + 1]) AS i8,
    toUInt32([4294967295, 0][n + 1]) AS u32,
    toInt64([1099511627776, -3][n + 1]) AS i64,
    toFloat32([1.5, -2.25][n + 1]) AS f32,
    toFloat64([0.1, 1e300][n + 1]) AS f64,
    toDate(['2020-01-01', '1970-01-02'][n + 1]) AS d,
    toDate32(['1950-01-01', '2100-12-31'][n + 1]) AS d32,
    toDateTime(['2020-01-01 00:00:00', '2001-02-03 04:05:06'][n + 1], 'UTC') AS dt,
    toDateTime64(['2020-01-01 00:00:00.123', '1969-12-31 23:59:59.999'][n + 1], 3, 'UTC') AS dt64_3,
    toDateTime64(['2020-01-01 00:00:00.123456', '1900-01-01 00:00:00.000001'][n + 1], 6, 'UTC') AS dt64_6,
    ['hello', 'Привет'][n + 1] AS s,
    toUUID(['00112233-4455-6677-8899-aabbccddeeff', 'f0e1d2c3-b4a5-9687-7869-5a4b3c2d1e0f'][n + 1]) AS uuid,
    toDecimal32(['123.45', '-1.00'][n + 1], 2) AS dec32,
    toDecimal128(['1234567890123456789.0123456789', '-0.0000000001'][n + 1], 10) AS dec128
FROM (SELECT number AS n FROM numbers(2))
"

$CLICKHOUSE_LOCAL -q "
SELECT
    ['b', 'i8', 'u32', 'i64', 'f32', 'f64', 'd', 'd32', 'dt', 'dt64_3', 'dt64_6', 's', 'uuid', 'dec32', 'dec128'][m.fields[1]],
    reinterpretAsUInt32(substring(blob, 9, 4)),
    hex(substring(blob, (reinterpretAsUInt8(substring(blob, 1, 1)) % 64) * 8 + 1))
FROM file('$OUT/reference.puffin', PuffinMetadata) AS m, file('$OUT/reference.puffin', RawBLOB, 'raw String') AS r
ARRAY JOIN [substring(r.raw, m.offset + 1, m.length)] AS blob
ORDER BY m.offset
"

rm -rf "${OUT:?}"
