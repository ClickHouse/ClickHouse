#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The schema reader and the parser of a plain `CSV` auto-detect a header by different rules. With the
# destination `(s String, n UInt8)` and the rows `n,"1"` and `bar,1.5`, the schema reader takes the
# first row for column names (the next row holds a number), while the parser reads it as data
# (`{n, 1}` and `{s, n}` are not a subset of one another) and fails only on the fractional `1.5`.
# The explanation must not blame a structure mismatch for that value-level error.

PHRASE="does not match the structure expected by the query"

check() {
    local output
    output=$(cat)
    if [ -z "$output" ]; then echo "ok"; return; fi
    if echo "$output" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

for detect in 1 0; do
    echo "-- input_format_csv_detect_header = $detect, fractional value in the second row"
    printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_csv_detect_header = %s FORMAT CSV\nn,"1"\nbar,1.5\n' "$detect" \
        | $CLICKHOUSE_LOCAL 2>&1 | check

    echo "-- input_format_csv_detect_header = $detect, valid control"
    printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_csv_detect_header = %s FORMAT CSV\nn,"1"\nbar,1\n' "$detect" \
        | $CLICKHOUSE_LOCAL 2>&1 | check
done

echo "-- a header that the parser detects as well, with a real structure mismatch"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\ns,n\n1,abc\n2,def\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
