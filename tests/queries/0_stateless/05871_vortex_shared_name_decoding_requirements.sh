#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With case-insensitive matching, header columns of different types can share a name. They are all taken from
# the one field of the file, which is decoded with the requirements of all of them, in whichever order they are
# requested: a numeric type reads a date as its day number, here one that is out of the range of `Date32`.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.vortex"

$CLICKHOUSE_LOCAL --query "
    SELECT d AS t, CAST(tuple(d) AS Tuple(d Date32)) AS s, [d] AS a
    FROM format(RowBinary, 'd Date32', unhex('c0c62d00'))
    INTO OUTFILE '$FILE' TRUNCATE FORMAT Vortex"

for structure in \
    "T String, t Int32" "t Int32, T String" \
    "S Tuple(d String), s Tuple(d Int32)" "s Tuple(d Int32), S Tuple(d String)" \
    "A Array(String), a Array(Int32)" "a Array(Int32), A Array(String)"
do
    echo "$structure"
    $CLICKHOUSE_LOCAL --query "
        SELECT * FROM file('$FILE', Vortex, '$structure')
        SETTINGS input_format_arrow_case_insensitive_column_matching = 1"
done

rm -f "$FILE"
