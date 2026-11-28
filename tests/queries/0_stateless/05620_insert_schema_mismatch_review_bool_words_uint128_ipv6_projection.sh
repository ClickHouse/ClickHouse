#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Parquet` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the INSERT structure-mismatch diagnostic.
# 1. The flat-text formats read a `Bool` with `SerializationBool`, which accepts the word forms
#    (`yes`, `on`, ...) that schema inference keeps as `String`: such a value is not a mismatch
#    when the row fails on a sibling column, while a value that is not a `Bool` text still is.
# 2. `Native` with `input_format_native_allow_types_conversion` casts a `UInt128` column into
#    `IPv6`, so it is not a mismatch either, while another numeric type still is.
# 3. A columnar file is sampled only for the columns the diagnostic needs.

PHRASE="does not match the structure expected by the query"

run() {
    local out
    out=$($CLICKHOUSE_LOCAL 2>&1)
    if echo "$out" | grep -q "Code:"; then echo -n "error "; else echo -n "ok "; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- TSV: a Bool word form next to an invalid sibling, and the successful control"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_tsv_detect_header = 0 FORMAT TSV\nyes\t1.5\n' | run
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_tsv_detect_header = 0 FORMAT TSV\nyes\t1\n' | run
echo "-- CSV: an unquoted Bool word form next to an invalid sibling, and the successful control"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_csv_detect_header = 0 FORMAT CSV\non,1.5\n' | run
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_csv_detect_header = 0 FORMAT CSV\non,1\n' | run
echo "-- TSV: a value that is not a Bool text is still a mismatch"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_tsv_detect_header = 0 FORMAT TSV\nabc\t1.5\n' | run
echo "-- JSONEachRow: a string token for a Bool column is still a mismatch"
printf 'CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"b":"yes","n":1.5}\n' | run

DATA_BAD=$CLICKHOUSE_TMP/data_05620_bad.native
DATA_GOOD=$CLICKHOUSE_TMP/data_05620_good.native
DATA_UINT64=$CLICKHOUSE_TMP/data_05620_uint64.native
$CLICKHOUSE_LOCAL -q "SELECT toUInt128(1) AS ip, 'not-a-uuid' AS u FORMAT Native" > "$DATA_BAD"
$CLICKHOUSE_LOCAL -q "SELECT toUInt128(1) AS ip, '61f0c404-5cb3-11e7-907b-a6006ad3dba0' AS u FORMAT Native" > "$DATA_GOOD"
$CLICKHOUSE_LOCAL -q "SELECT toUInt64(1) AS ip, 'not-a-uuid' AS u FORMAT Native" > "$DATA_UINT64"

echo "-- Native: a UInt128 column for an IPv6 column next to an invalid sibling, and the successful control"
for data in "$DATA_BAD" "$DATA_GOOD"; do
    {
        echo "CREATE TABLE t (ip IPv6, u UUID) ENGINE = Memory; INSERT INTO t SETTINGS input_format_native_allow_types_conversion = 1 FORMAT Native"
        cat "$data"
    } | run
done
echo "-- Native: a UInt64 column for an IPv6 column fails the cast (not a parse error)"
{
    echo "CREATE TABLE t (ip IPv6, u UUID) ENGINE = Memory; INSERT INTO t SETTINGS input_format_native_allow_types_conversion = 1 FORMAT Native"
    cat "$DATA_UINT64"
} | run

echo "-- Parquet: a large unused column is not decoded for the diagnostic"
DATA_PARQUET=$CLICKHOUSE_TMP/data_05620.parquet
$CLICKHOUSE_LOCAL -q "SELECT arrayStringConcat(arrayMap(x -> repeat('x', 1000000), range(50))) AS unused, 'not-a-uuid' AS u FORMAT Parquet SETTINGS output_format_parquet_compression_method = 'zstd'" > "$DATA_PARQUET"
{
    echo "CREATE TABLE t (u UUID) ENGINE = Memory; INSERT INTO t FORMAT Parquet"
    cat "$DATA_PARQUET"
} | $CLICKHOUSE_LOCAL 2>&1 | grep -o -m1 "CANNOT_PARSE_UUID"

rm -f "$DATA_BAD" "$DATA_GOOD" "$DATA_UINT64" "$DATA_PARQUET"
