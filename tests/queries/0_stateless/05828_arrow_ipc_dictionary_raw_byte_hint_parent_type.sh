#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The values of a dictionary-encoded `fixed_size_binary(16)` child are converted when a dotted header column
# such as `t.u UUID` requests a raw-byte type for it. When the parent `t` is requested too, with a type that
# does not describe `u` (here `String`), the decoded `t` must declare its child with the converted type: before
# the fix a `UUID` child was still declared as `FixedString(16)`, and the cast of `t` read the `UUID` column as a
# `FixedString` one. Since the field is decoded for two requested columns, the dictionary values keep their bytes,
# and each column is converted by its own type: `t` reads the raw bytes of `u`, as it does when requested alone.

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"

$CLICKHOUSE_LOCAL -q "
    SELECT CAST(tuple(number % 2 = 1, toLowCardinality(toFixedString(unhex('20010db800000000000000000000000' || toString(number + 1)), 16))) AS Tuple(c0 Bool, u LowCardinality(FixedString(16)))) AS t,
        [CAST(tuple(toLowCardinality(toFixedString(unhex('00000000000000000000ffff0102030' || toString(number)), 16))) AS Tuple(u LowCardinality(FixedString(16))))] AS a
    FROM numbers(3)
    INTO OUTFILE '${DATA}' TRUNCATE FORMAT ArrowStream SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1"

echo "-- parent String and child UUID"
$CLICKHOUSE_LOCAL -q "SELECT t, \`t.u\`, toTypeName(\`t.u\`) FROM file('${DATA}', ArrowStream, 't String, \`t.u\` UUID')"
echo "-- parent String and child IPv6"
$CLICKHOUSE_LOCAL -q "SELECT t, \`t.u\` FROM file('${DATA}', ArrowStream, 't String, \`t.u\` IPv6')"
echo "-- parent String and child UInt128"
$CLICKHOUSE_LOCAL -q "SELECT t, \`t.u\` FROM file('${DATA}', ArrowStream, 't String, \`t.u\` UInt128')"
echo "-- struct inside a list: parent String and child Array(UUID)"
$CLICKHOUSE_LOCAL -q "SELECT a, \`a.u\` FROM file('${DATA}', ArrowStream, 'a String, \`a.u\` Array(UUID)')"

rm -f "${DATA}"
