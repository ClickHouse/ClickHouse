#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ORC is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The child of an empty nested array or map has an empty range of rows, whose end offset was indexed as
# an unsigned `end - 1` (which UBSan now reports) in the native ORC and Arrow IPC writers.

for format in ORC ArrowStream; do
    echo "-- $format"
    $CLICKHOUSE_LOCAL -q "
    SELECT materialize(CAST([], 'Array(Array(UInt64))')) AS x, materialize(CAST([], 'Array(Map(String, UInt64))')) AS y,
        materialize(CAST(map(), 'Map(String, Array(UInt64))')) AS z
    FORMAT $format" | $CLICKHOUSE_LOCAL --input-format $format -q "SELECT * FROM table"
    $CLICKHOUSE_LOCAL -q "
    SELECT if(number = 1, [[number]], []) AS x, if(number = 1, [map('a', number)], []) AS y FROM numbers(3)
    FORMAT $format" | $CLICKHOUSE_LOCAL --input-format $format -q "SELECT * FROM table"
done
