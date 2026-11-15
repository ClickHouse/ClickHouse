#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ArrowStream` format needs Arrow, which is not built in the fast test.

# A struct field of an `ArrowStream` file requested both as a whole column of another type and as a
# subcolumn with a type that reinterprets its raw bytes (`IPv6`) must be decoded consistently for both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
$CLICKHOUSE_LOCAL -q "SELECT CAST(tuple(false, 'abcdefghijklmnop') AS Tuple(c0 Bool, s String)) AS t FROM numbers(3) FORMAT ArrowStream" > "$file"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't String, \`t.s\` IPv6')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool), \`t.s\` IPv6')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', 't Tuple(c0 Bool, s String), \`t.s\` IPv6') LIMIT 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file', 'ArrowStream', '\`t.s\` IPv6') LIMIT 1"

rm -f "$file"
