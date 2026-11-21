#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dotted header column such as `t.s IPv6` makes the Arrow IPC reader decode the 16-byte binary child `s`
# of the struct `t` as `IPv6`. When the parent `t` is requested too, with a type that does not describe `s`
# (here `String`), the decoded `t` must declare its child with the type of its column: before the fix it still
# declared `String`, and the cast of `t` read the `IPv6` column as a `String` one. Since the field is decoded for
# two requested columns, its bytes are kept, and each column is converted by its own type: `t` reads the raw
# bytes of `s`, as it does when requested alone.

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_LOCAL -q "
    SELECT CAST(tuple(number % 2 = 1, unhex('20010db800000000000000000000000' || toString(number + 1))) AS Tuple(c0 Bool, s String)) AS t,
        [CAST(tuple(unhex('00000000000000000000ffff0102030' || toString(number))) AS Tuple(s String))] AS a
    FROM numbers(3)
    INTO OUTFILE '${DATA}.struct' TRUNCATE FORMAT ArrowStream"

# Same layout, but the struct children are dictionary-encoded.
$CLICKHOUSE_LOCAL -q "
    SELECT CAST(tuple(number % 2 = 1, toLowCardinality(unhex('20010db800000000000000000000000' || toString(number + 1)))) AS Tuple(c0 Bool, s LowCardinality(String))) AS t
    FROM numbers(3)
    INTO OUTFILE '${DATA}.dict' TRUNCATE FORMAT ArrowStream SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1"

echo "-- parent String and child IPv6"
$CLICKHOUSE_LOCAL -q "SELECT t, \`t.s\`, toTypeName(\`t.s\`) FROM file('${DATA}.struct', ArrowStream, 't String, \`t.s\` IPv6')"
echo "-- child IPv6 only"
$CLICKHOUSE_LOCAL -q "SELECT \`t.s\` FROM file('${DATA}.struct', ArrowStream, '\`t.s\` IPv6')"
echo "-- parent Tuple without the child and child IPv6"
$CLICKHOUSE_LOCAL -q "SELECT t, \`t.s\` FROM file('${DATA}.struct', ArrowStream, 't Tuple(c0 Bool), \`t.s\` IPv6')"
echo "-- struct inside a list: parent String and child Array(IPv6)"
$CLICKHOUSE_LOCAL -q "SELECT a, \`a.s\` FROM file('${DATA}.struct', ArrowStream, 'a String, \`a.s\` Array(IPv6)')"
echo "-- dictionary-encoded child: parent String and child IPv6"
$CLICKHOUSE_LOCAL -q "SELECT hex(t), \`t.s\` FROM file('${DATA}.dict', ArrowStream, 't String, \`t.s\` IPv6')"

rm -f "${DATA}.struct" "${DATA}.dict"
