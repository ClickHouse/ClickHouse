#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `input_format_json_map_as_array_of_tuples` an empty array is a valid empty `Map`. Schema
# inference types `[]` as an array of a placeholder `String`, which must not be taken for a malformed
# pair: the structure-mismatch explanation must not be attached to the parse error of a sibling column.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    printf '%s' "$1" | $CLICKHOUSE_LOCAL --input_format_json_map_as_array_of_tuples=1 \
        --query "CREATE TABLE t ($2) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
}

echo "-- an empty map, the sibling column fails"
run '{"m":[],"n":1.5}' 'm Map(String, UInt8), n UInt8'

echo "-- the successful control"
printf '%s' '{"m":[],"n":1}' | $CLICKHOUSE_LOCAL --input_format_json_map_as_array_of_tuples=1 \
    --structure 'm Map(String, UInt8), n UInt8' --input-format JSONEachRow --query "SELECT * FROM table"

echo "-- an empty map at a nested path, the sibling column fails"
run '{"t":{"m":[]},"n":1.5}' 't Tuple(m Map(String, UInt8)), n UInt8'
run '{"a":[[],[]],"n":1.5}' 'a Array(Map(String, UInt8)), n UInt8'

echo "-- a non-empty array that is not an array of pairs is still explained"
run '{"m":["a"],"n":1}' 'm Map(String, UInt8), n UInt8'
run '{"m":[],"n":1}
{"m":["a"],"n":1}' 'm Map(String, UInt8), n UInt8'
