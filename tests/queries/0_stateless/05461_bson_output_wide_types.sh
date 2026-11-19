#!/usr/bin/env bash

# BSONEachRow output of the wide numeric types and of LowCardinality.
#
# Expected behaviour: the "Data types matching" table of the BSONEachRow
# documentation (doc comment at the end of
# src/Processors/Formats/Impl/BSONEachRowRowInputFormat.cpp), which specifies
#   Date32                     -> \x10 int32
#   Decimal64                  -> \x12 int64
#   Int128/UInt128/Decimal128  -> \x05 binary, \x00 subtype, size = 16
#   Int256/UInt256/Decimal256  -> \x05 binary, \x00 subtype, size = 32
#   String                     -> \x05 binary, \x00 subtype
#                                 (\x02 string with output_format_bson_string_as_string)
# LowCardinality(String) is written as its dictionary value, so like String.
#
# This has to be a .sh test: the format writes binary output, which must be
# captured in a file before it can be inspected and read back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.bson"

# Extreme values so that each payload is unambiguous: a Date32 before 1970,
# which is a negative day number that a Date column cannot hold, the minimum
# or maximum of every big integer type, and negative decimals. The big
# integers are given as strings because a bare 39-digit literal would be
# parsed as a Float64 and lose precision.
$CLICKHOUSE_CLIENT -q "
SELECT
    toLowCardinality('bson') AS lc,
    toDate32('1900-01-01') AS d32,
    toInt128('-170141183460469231731687303715884105728') AS i128,
    toUInt128('340282366920938463463374607431768211455') AS u128,
    toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') AS i256,
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') AS u256,
    toDecimal64('-1.25', 2) AS d64,
    toDecimal128('1.25', 2) AS d128,
    toDecimal256('-1.25', 2) AS d256
FORMAT BSONEachRow SETTINGS output_format_bson_string_as_string = 0
" > "$OUT"

echo '--- one element per column: type byte, binary size, binary subtype, integer payload ---'
$CLICKHOUSE_LOCAL -q "
SELECT
    name,
    hex(substring(raw, pos - 1, 1)) AS bson_type,
    if(bson_type = '05', toString(reinterpretAsUInt32(substring(raw, pos + length(name) + 1, 4))), '.') AS binary_size,
    if(bson_type = '05', hex(substring(raw, pos + length(name) + 5, 1)), '.') AS binary_subtype,
    multiIf(bson_type = '10', toString(reinterpretAsInt32(substring(raw, pos + length(name) + 1, 4))),
            bson_type = '12', toString(reinterpretAsInt64(substring(raw, pos + length(name) + 1, 8))),
            '.') AS int_payload
FROM
(
    -- a BSON element is <type byte><key name><NUL><payload>
    SELECT raw, name, position(raw, concat(name, '\0')) AS pos
    FROM file('$OUT', 'RawBLOB', 'raw String')
    ARRAY JOIN ['lc', 'd32', 'i128', 'u128', 'i256', 'u256', 'd64', 'd128', 'd256'] AS name
)
SETTINGS input_format_parallel_parsing = 0
"

echo '--- document framing: the size prefix is the document length, the terminator is NUL ---'
$CLICKHOUSE_LOCAL -q "
SELECT
    reinterpretAsUInt32(substring(raw, 1, 4)) = length(raw) AS size_prefix_ok,
    hex(substring(raw, length(raw), 1)) AS terminator
FROM file('$OUT', 'RawBLOB', 'raw String')
SETTINGS input_format_parallel_parsing = 0
"

echo '--- read back with the original types ---'
$CLICKHOUSE_LOCAL -q "
SELECT * FROM file('$OUT', 'BSONEachRow',
    'lc LowCardinality(String), d32 Date32, i128 Int128, u128 UInt128,
     i256 Int256, u256 UInt256, d64 Decimal64(2), d128 Decimal128(2), d256 Decimal256(2)')
FORMAT Vertical
SETTINGS input_format_parallel_parsing = 0, output_format_pretty_single_large_number_tip_threshold = 0
"

rm -f "$OUT"
