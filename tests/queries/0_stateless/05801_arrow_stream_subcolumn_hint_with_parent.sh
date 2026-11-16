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

rm -f "$file"
