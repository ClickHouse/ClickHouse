#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ArrowStream` format needs Arrow, which is not built in the fast test.

# A struct field of an `ArrowStream` file requested both as a whole column and as a subcolumn, with types
# that read its raw bytes differently (`IPv6`, a big integer, `String`), must be decoded consistently for both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
$CLICKHOUSE_LOCAL -q "SELECT CAST(tuple(false, 'abcdefghijklmnop') AS Tuple(c0 Bool, s String)) AS t FROM numbers(3) FORMAT ArrowStream" > "$file"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.s\` IPv6')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool), \`t.s\` IPv6')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s String), \`t.s\` IPv6') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', '\`t.s\` IPv6') LIMIT 1"

echo "reverse direction"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), \`t.s\` String') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), \`t.s\` UInt128') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), \`t.s\` IPv6') LIMIT 1"

echo "dictionary"
$CLICKHOUSE_LOCAL -q "SELECT CAST(tuple(false, 'abcdefghijklmnop') AS Tuple(c0 Bool, s LowCardinality(String))) AS t FROM numbers(3) FORMAT ArrowStream SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.s\` IPv6') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), \`t.s\` String') LIMIT 1"

echo "nested"
$CLICKHOUSE_LOCAL -q "SELECT CAST(tuple(tuple('abcdefghijklmnop')) AS Tuple(u Tuple(s String))) AS t FROM numbers(3) FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(u Tuple(s IPv6)), \`t.u\` String') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.u\` Tuple(s IPv6)') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', '\`t.u\` Tuple(s IPv6), \`t.u.s\` String') LIMIT 1"

echo "date range of a subcolumn"
$CLICKHOUSE_LOCAL -q "SELECT CAST(tuple(false, toDate32('1969-12-31')) AS Tuple(c0 Bool, d Date32)) AS t FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool), \`t.d\` Date') SETTINGS date_time_overflow_behavior = 'throw'" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool), \`t.d\` Date') SETTINGS date_time_overflow_behavior = 'saturate'"

echo "dictionary of 16-byte values only"
# Written by PyArrow: a struct child encoded with a dictionary that holds only `abcdefghijklmnop`, so the
# dictionary-wide width check does not decline the conversion (the ClickHouse writer adds an empty entry).
echo "//////AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAkP///wQAAAABAAAABAAAAHj///8AAAENGAAAABwAAAAEAAAAAgAAAIAAAAAgAAAAAQAAAHQAAABw////EAAYAAgABgAHAAwAEAAUABAAAAAAAAEEFAAAADwAAAAcAAAABAAAAAAAAAABAAAAcwAAAAgACAAAAAQACAAAAAwAAAAIAAwACAAHAAgAAAAAAAABIAAAAMz///8QABQACAAGAAcADAAAABAAEAAAAAAAAQYQAAAAGAAAAAQAAAAAAAAAAgAAAGMwAAAEAAQABAAAAAAAAAD/////qAAAABQAAAAAAAAADAAUAAYABQAIAAwADAAAAAACBAAUAAAAGAAAAAAAAAAIAAoAAAAEAAgAAAAQAAAAAAAKABgADAAEAAgACgAAAEwAAAAQAAAAAQAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAQAAAAAAAAAAAAAAABAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAYWJjZGVmZ2hpamtsbW5vcP/////YAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAYAAAAAAAAAAAACgAYAAwABAAIAAoAAABsAAAAEAAAAAMAAAAAAAAAAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAMAAAAAAAAAAAAAAADAAAAAwAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/////wAAAAA=" | base64 -d > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), \`t.s\` String') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.s\` IPv6') LIMIT 1"

echo "map keys and values"
$CLICKHOUSE_LOCAL -q "SELECT map('abcdefghijklmnop', 1) AS t FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(IPv6, UInt8), \`t.keys\` Array(String)')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(String, UInt8), \`t.keys\` Array(IPv6)')"
$CLICKHOUSE_LOCAL -q "SELECT map(1, 'abcdefghijklmnop') AS t FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(UInt8, IPv6), \`t.values\` Array(String)')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(UInt8, String), \`t.values\` Array(IPv6)')"

echo "nested map keys and values"
$CLICKHOUSE_LOCAL -q "SELECT CAST(map(tuple('abcdefghijklmnop'), 1) AS Map(Tuple(s String), UInt8)) AS t FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(Tuple(s IPv6), UInt8), \`t.keys.s\` Array(String)')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(Tuple(s String), UInt8), \`t.keys.s\` Array(IPv6)')"
$CLICKHOUSE_LOCAL -q "SELECT CAST(map(1, tuple('abcdefghijklmnop')) AS Map(UInt8, Tuple(s String))) AS t FORMAT ArrowStream" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(UInt8, Tuple(s IPv6)), \`t.values.s\` Array(String)')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Map(UInt8, Tuple(s String)), \`t.values.s\` Array(IPv6)')"

echo "null struct row"
$CLICKHOUSE_LOCAL -q "SELECT CAST(if(number = 1, NULL, tuple('abcdefghijklmnop')) AS Nullable(Tuple(s String))) AS t FROM numbers(2) FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.s\` IPv6') SETTINGS enable_nullable_tuple_type = 0"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(s IPv6), \`t.s\` String') SETTINGS enable_nullable_tuple_type = 0"

echo "null outer struct row"
$CLICKHOUSE_LOCAL -q "SELECT CAST(if(number = 1, NULL, tuple(tuple('abcdefghijklmnop'))) AS Nullable(Tuple(u Tuple(s String)))) AS t FROM numbers(2) FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1" > "$file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', '\`t.u\` String, \`t.u.s\` IPv6') SETTINGS enable_nullable_tuple_type = 0"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', '\`t.u\` Tuple(s IPv6), \`t.u.s\` String') SETTINGS enable_nullable_tuple_type = 0"

rm -f "$file"
