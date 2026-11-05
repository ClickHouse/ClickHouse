#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `sipHash64` and `sipHash128` of every type, of nested types, of several arguments and of constants. The checksums in
# the reference were computed by the code that hashes one row at a time, so they must not change. Every value is made
# from `number`, and the same query runs with blocks of 1, 7, 9, 257, 1003 and 65536 rows, so the reference repeats the
# checksums once per block size.

# Special values of an integer type for `k` < 5 (0, 1, all bits set, the maximum and the minimum of the signed type),
# random bits otherwise.
ints() { echo "multiIf(k = 0, 0, k = 1, 1, k = 2, $1, k = 3, $2, k = 4, $3, h)"; }
big()
{
    local max="bitNot(toUInt$1(0))"
    echo "multiIf(k = 0, toUInt$1(0), k = 1, toUInt$1(1), k = 2, $max, k = 3, bitShiftRight($max, 1), k = 4, bitNot(bitShiftRight($max, 1)), reinterpretAsUInt$1(b))"
}
I64=$(ints 0xFFFFFFFFFFFFFFFF 0x7FFFFFFFFFFFFFFF 0x8000000000000000)
V='Variant(UInt64, String, Array(UInt32))'
JSON1='JSON(max_dynamic_paths = 1, a.b UInt32)'

# One line per type: the name in the output and the arguments of the hash functions.
TYPES=$(cat <<EOF
UInt8|toUInt8($(ints 0xFF 0x7F 0x80))
UInt16|toUInt16($(ints 0xFFFF 0x7FFF 0x8000))
UInt32|toUInt32($(ints 0xFFFFFFFF 0x7FFFFFFF 0x80000000))
UInt64|toUInt64($I64)
UInt128|$(big 128)
UInt256|$(big 256)
Int8|toInt8($(ints 0xFF 0x7F 0x80))
Int16|toInt16($(ints 0xFFFF 0x7FFF 0x8000))
Int32|toInt32($(ints 0xFFFFFFFF 0x7FFFFFFF 0x80000000))
Int64|toInt64($I64)
Int128|reinterpretAsInt128($(big 128))
Int256|reinterpretAsInt256($(big 256))
Float32|reinterpretAsFloat32(toUInt32(multiIf(k = 0, 0, k = 1, 0x80000000, k = 2, 0x7FC00000, k = 3, 0xFFC00001, k = 4, 0x7F800000, k = 5, 0xFF800000, k = 6, 1, k = 7, 0x807FFFFF, h)))
Float64|f64
BFloat16|reinterpret(toUInt16(multiIf(k = 0, 0, k = 1, 0x8000, k = 2, 0x7FC0, k = 3, 0xFFC1, k = 4, 0x7F80, k = 5, 0xFF80, k = 6, 1, k = 7, 0x807F, h)), 'BFloat16')
Bool|CAST(number % 3 = 0 AS Bool)
Decimal32|reinterpret(toInt32(multiIf(k = 0, 0, k = 1, 999999999, k = 2, -999999999, k = 3, -1, toInt64(h % 1999999999) - 999999999)), 'Decimal32(4)')
Decimal64|reinterpret(toInt64(multiIf(k = 0, 0, k = 1, 999999999999999999, k = 2, -999999999999999999, k = 3, -1, toInt64(h % 1999999999999999999) - 999999999999999999)), 'Decimal64(6)')
Decimal128|reinterpret(multiIf(k = 0, toInt128(0), k = 1, toInt128(repeat('9', 38)), k = 2, -toInt128(repeat('9', 38)), k = 3, toInt128(-1), reinterpretAsInt128(b) % toInt128(concat('1', repeat('0', 38)))), 'Decimal128(10)')
Decimal256|reinterpret(multiIf(k = 0, toInt256(0), k = 1, toInt256(repeat('9', 76)), k = 2, -toInt256(repeat('9', 76)), k = 3, toInt256(-1), reinterpretAsInt256(b) % toInt256(concat('1', repeat('0', 76)))), 'Decimal256(20)')
Date|reinterpret(toUInt16(multiIf(k = 0, 0, k = 1, 0xFFFF, h)), 'Date')
Date32|toDate32('1900-01-01') + toInt32(multiIf(k = 0, 0, k = 1, 146096, k = 2, 25567, h % 146097))
DateTime|reinterpret(toUInt32(multiIf(k = 0, 0, k = 1, 0xFFFFFFFF, h)), 'DateTime')
DateTime64(3)|reinterpret(toInt64(multiIf(k = 0, 0, k = 1, -2208988800000, k = 2, 10413791999999, toInt64(h % 12622780800000) - 2208988800000)), 'DateTime64(3)')
Time|toTime(toInt32(multiIf(k = 0, 0, k = 1, -3599999, k = 2, 3599999, toInt64(h % 7199999) - 3599999)))
Time64(6)|toTime64(reinterpret(toInt64(multiIf(k = 0, 0, k = 1, -3599999999999, k = 2, 3599999999999, toInt64(h % 7199999999999) - 3599999999999)), 'Decimal64(6)'), 6)
Enum8|CAST(['a', 'b', 'c'][number % 3 + 1] AS Enum8('a' = -128, 'b' = 0, 'c' = 127))
Enum16|CAST(['a', 'b', 'c', 'd'][number % 4 + 1] AS Enum16('a' = -32768, 'b' = 0, 'c' = 1000, 'd' = 32767))
UUID|reinterpret($(big 128), 'UUID')
IPv4|toIPv4(toUInt32(multiIf(k = 0, 0, k = 1, 0xFFFFFFFF, h)))
IPv6|toIPv6($(big 128))
IntervalSecond|toIntervalSecond(toInt64($I64))
String|str
FixedString(1)|toFixedString(substring(str, 1, 1), 1)
FixedString(5)|toFixedString(substring(str, 1, 5), 5)
FixedString(8)|toFixedString(substring(str, 1, 8), 8)
FixedString(16)|toFixedString(substring(str, 1, 16), 16)
FixedString(33)|toFixedString(substring(str, 1, 33), 33)
Nullable(UInt32)|nu32
Nullable(String)|nstr
LowCardinality(String)|toLowCardinality(str)
LowCardinality(Nullable(String))|toLowCardinality(nstr)
Array(UInt8)|au8
Array(String)|arrayMap(i -> substring(str, i + 1), range(number % 5))
Array(Array(UInt32))|arrayMap(i -> arrayMap(j -> toUInt32(h + i * 31 + j), range((intDiv(number, 4) + i) % 3)), range(number % 4))
Array(Nullable(String))|arrayMap(i -> if((number + i) % 7 = 0, NULL, substring(str, i + 1)), range(number % 5))
Array(Tuple(UInt64, String))|arrayMap(i -> (h + i, substring(str, i + 1)), range(number % 4))
Map(String, UInt64)|mapFromArrays(arrayMap(i -> substring(str, 1, i), range(number % 6)), arrayMap(i -> h + i, range(number % 6)))
Map(UInt32, Array(String))|mapFromArrays(arrayMap(i -> toUInt32(h + i), range(number % 4)), arrayMap(i -> arrayMap(j -> substring(str, j + 1), range((number + i) % 3)), range(number % 4)))
Array(UInt32)|arrayMap(i -> toUInt32(h + i), range(multiIf(number % 97 = 0, 1000 + number % 50, number % 301)))
Map(UInt64, String)|mapFromArrays(arrayMap(i -> h + i, range(number % 41)), arrayMap(i -> substring(str, i + 1), range(number % 41)))
Tuple(UInt64, String, Float64)|(h, str, f64)
Tuple(Nullable(UInt32), Nullable(String))|(nu32, nstr)
Tuple(LowCardinality(String))|tuple(toLowCardinality(str))
Empty Tuple|tuple()
Nested Tuple|((h, (str, toUInt8(number))), tuple(toInt16(h)), tuple())
$V|multiIf(number % 4 = 0, NULL, number % 4 = 1, CAST(h AS $V), number % 4 = 2, CAST(str AS $V), CAST(au32 AS $V))
Dynamic|dyn
Dynamic(max_types = 1)|CAST(dyn AS Dynamic(max_types = 1))
Tuple(Dynamic)|tuple(dyn)
Tuple(Dynamic(max_types = 1))|tuple(CAST(dyn AS Dynamic(max_types = 1)))
JSON|js
$JSON1|CAST(js AS $JSON1)
Tuple(JSON)|tuple(js)
Tuple($JSON1)|tuple(CAST(js AS $JSON1))
AggregateFunction(sum, UInt64)|arrayReduce('sumState', arrayMap(i -> h + i, range(number % 5)))
AggregateFunction(uniq, String)|arrayReduce('uniqState', arrayMap(i -> toString(h + i), range(number % 40)))
QBit(Float32, 8)|CAST(arrayMap(i -> reinterpretAsFloat32(toUInt32(cityHash64(number, i))), range(8)) AS QBit(Float32, 8))
NULL|NULL
Empty Array|[]
UInt64, String, Nullable(Float64), Array(UInt8)|h, str, nf64, au8
String and constant String|str, 'c'
Constant UInt32 and String|toUInt32(5), str
Constant Tuple|(1, 'abc', [2, 3])
String and constant Tuple|str, (1, 'abc', [2, 3])
EOF
)

# `str` has every length from 0 to 300 several times, then about 4 KB or short. `f64` has signed zeros, NaNs, infinities
# and subnormals. The values of `dyn` have 6 types and NULL. The arrays in `js` are never empty, because the type of an
# empty array in JSON comes from the other rows of the block.
SOURCE="
    SELECT
        number,
        cityHash64(number) AS h,
        cityHash64(number, 1) AS g,
        number % 13 AS k,
        concat(reinterpretAsFixedString(h), reinterpretAsFixedString(g),
            reinterpretAsFixedString(cityHash64(number, 2)), reinterpretAsFixedString(cityHash64(number, 3))) AS b,
        unhex(substring(repeat(hex(b), 126), 1,
            2 * multiIf(number < 2000, number % 301, number % 3 = 0, 4000 + number % 9, number % 64))) AS str,
        reinterpretAsFloat64(multiIf(k = 0, 0, k = 1, 0x8000000000000000, k = 2, 0x7FF8000000000000, k = 3, 0xFFF8000000000001,
            k = 4, 0x7FF0000000000000, k = 5, 0xFFF0000000000000, k = 6, 1, k = 7, 0x800FFFFFFFFFFFFF, h)) AS f64,
        if(number % 7 = 0, NULL, toUInt32(h)) AS nu32,
        if(number % 7 = 0, NULL, str) AS nstr,
        if(number % 7 = 3, NULL, f64) AS nf64,
        arrayMap(i -> toUInt8(h + i), range(number % 9)) AS au8,
        arrayMap(i -> toUInt32(g + i), range(number % 5)) AS au32,
        multiIf(number % 7 = 0, NULL::Dynamic, number % 7 = 1, h::Dynamic, number % 7 = 2, str::Dynamic, number % 7 = 3, au32::Dynamic,
            number % 7 = 4, f64::Dynamic, number % 7 = 5, (h, str)::Dynamic, (toDate32('1900-01-01') + toInt32(h % 146097))::Dynamic) AS dyn,
        CAST(if(k = 12, '{}', concat('{\"a\":{\"b\":', toString(toUInt32(h)), '},\"k', toString(number % 5), '\":', toString(number),
            ',\"s', toString(number % 3), '\":\"', hex(substring(str, 1, number % 9)), '\",\"arr', toString(number % 2), '\":[',
            arrayStringConcat(arrayMap(i -> toString(number + i), range(1 + number % 4)), ','), '],\"f\":', toString(number % 10 / 4), '}'))
            AS JSON(a.b UInt32)) AS js
    FROM numbers(3003)"

# Full column names and no readable number tips in the output.
FORMAT="output_format_pretty_max_column_name_width_cut_to = 0, output_format_pretty_single_large_number_tip_threshold = 0 FORMAT Vertical"

COLUMNS=""
while IFS='|' read -r name args; do
    COLUMNS+=", groupBitXor(cityHash64(number, sipHash64($args))) AS \`$name sipHash64\`"
    COLUMNS+=", groupBitXor(cityHash64(number, sipHash128($args))) AS \`$name sipHash128\`"
done <<< "$TYPES"

for block_size in 1 7 9 257 1003 65536; do
    $CLICKHOUSE_CLIENT -q "SELECT $block_size AS max_block_size $COLUMNS FROM ($SOURCE) SETTINGS max_block_size = $block_size, $FORMAT"
done

# The keyed and reference variants share code paths with the functions above.
KEYED=""
while IFS='|' read -r name args; do
    KEYED+=", groupBitXor(cityHash64(number, sipHash64Keyed((toUInt64(1), toUInt64(2)), $args))) AS \`$name sipHash64Keyed\`"
    KEYED+=", groupBitXor(cityHash64(number, sipHash128Keyed((toUInt64(1), toUInt64(2)), $args))) AS \`$name sipHash128Keyed\`"
    KEYED+=", groupBitXor(cityHash64(number, sipHash128Reference($args))) AS \`$name sipHash128Reference\`"
done <<< "$(grep -E '^(Tuple\(Nullable|Array\(String\)|Map\(String|UInt64, String)' <<< "$TYPES")"
$CLICKHOUSE_CLIENT -q "SELECT 'keyed' AS functions $KEYED FROM ($SOURCE) SETTINGS $FORMAT"
