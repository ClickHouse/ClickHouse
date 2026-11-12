#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Arrow format is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="SELECT number, multiIf(number = 0, 42::Variant(UInt8, String, Array(UInt8)), number = 1, 'a\"b', number = 2, [1, 2], NULL) AS v, [v, v] AS arr, tuple(v) AS t FROM numbers(4)"

for format in Arrow ArrowStream; do
    echo "${format}"
    $CLICKHOUSE_LOCAL -q "${query} FORMAT ${format} SETTINGS output_format_arrow_variant_as_string = 1" \
        | $CLICKHOUSE_LOCAL --input-format ${format} -q "DESCRIBE TABLE table SETTINGS describe_compact_output = 1; SELECT v, arr, t FROM table ORDER BY number"
done

echo "default"
$CLICKHOUSE_LOCAL -q "${query} FORMAT Arrow" | $CLICKHOUSE_LOCAL --input-format Arrow -q "SELECT v, toTypeName(v) FROM table ORDER BY number"
