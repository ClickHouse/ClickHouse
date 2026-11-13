#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The aggregation method of a `GROUP BY` over fixed-width keys and one `String` key. The width of the fixed-width
# part picks `keys32_packed_string`, `keys64_packed_string` or `keys128_packed_string`, also for `LowCardinality` keys.
# Keys outside the limits of these methods, a `LowCardinality(Nullable(String))` key, the setting off and a
# `compatibility` before 26.11 keep the serialized method. Each query is tagged with a `log_comment`, which the trace log
# prints before the aggregation method of the query.

# The query of case `$1`: a `GROUP BY` of the columns `$2` and a `count()`, with the extra settings `$3`.
function query()
{
    echo "SELECT *, count() FROM (SELECT $2 FROM numbers(100)) GROUP BY ALL SETTINGS log_comment = '$1'${3:+, $3} FORMAT Null;"
}

string="toString(intDiv(number, 3)) AS s"

queries="
$(query u8_u16_string "toUInt8(number % 3) AS a, toUInt16(number % 5) AS b, $string")
$(query u32_string "toUInt32(number % 7) AS a, $string")
$(query u32_u8_string "toUInt32(number % 7) AS a, toUInt8(number % 3) AS b, $string")
$(query u64_string "toUInt64(number % 7) AS a, $string")
$(query u64_u8_string "toUInt64(number % 7) AS a, toUInt8(number % 3) AS b, $string")
$(query u128_string "toUInt128(number % 7) AS a, $string")
$(query fixed_string_3_string "toFixedString(leftPad(toString(number % 7), 3, '0'), 3) AS a, $string")
$(query u128_u64_string "toUInt128(number % 7) AS a, toUInt64(number % 5) AS b, $string")
$(query u32_two_strings "toUInt32(number % 7) AS a, $string, toString(intDiv(number, 5)) AS t")
$(query u32_nullable_string "toUInt32(number % 7) AS a, toNullable(toString(intDiv(number, 3))) AS s")
$(query u32_low_cardinality_string "toUInt32(number % 7) AS a, toLowCardinality(toString(intDiv(number, 3))) AS s")
$(query low_cardinality_u32_string "toLowCardinality(toUInt32(number % 7)) AS a, $string")
$(query u32_low_cardinality_nullable_string "toUInt32(number % 7) AS a, toLowCardinality(toNullable(toString(intDiv(number, 3)))) AS s")
$(query u32_string_setting_off "toUInt32(number % 7) AS a, $string" "enable_fixed_and_packed_string_keys_in_aggregation = 0")
$(query u32_string_compatibility "toUInt32(number % 7) AS a, $string" "compatibility = '26.10'")
"

$CLICKHOUSE_LOCAL --send_logs_level=trace --query "$queries" 2>&1 \
    | grep -oE "comment: [a-z0-9_]+|Aggregation method: [a-z0-9_]+" | uniq
