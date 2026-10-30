#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A JSON array with elements of different types is inferred as an unnamed `Tuple`, and the parser reads
# it into a `String` column under `input_format_json_read_arrays_as_strings`, not under
# `input_format_json_read_objects_as_strings`. When only an unrelated value is invalid, no structure-mismatch
# explanation must be added to the error, and the control without the invalid value inserts.

PHRASE="does not match the structure expected by the query"

check() {
    local output
    output=$(cat)
    grep -o -E "\((CANNOT_PARSE_[A-Z_]+|CANNOT_READ_[A-Z_]+|INCORRECT_DATA|TYPE_MISMATCH)\)" <<< "$output" | head -n1
    if grep -q "$PHRASE" <<< "$output"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    $CLICKHOUSE_LOCAL "$@" --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 \
        && echo "inserted"
}

SETTINGS=(--input_format_json_infer_array_of_dynamic_from_array_of_different_types 0 --input_format_json_read_numbers_as_strings 0)

echo "-- Arrays read as strings, objects not: a heterogeneous array is valid for the String column"
ARRAYS=("${SETTINGS[@]}" --input_format_json_read_objects_as_strings 0 --input_format_json_read_arrays_as_strings 1)
echo '{"s":[1,"x"],"n":1.5}' | run "${ARRAYS[@]}" | check
echo '{"s":[1,"x"],"n":1}' | run "${ARRAYS[@]}"

echo "-- Objects read as strings, arrays not: a heterogeneous array is a structure mismatch"
OBJECTS=("${SETTINGS[@]}" --input_format_json_read_objects_as_strings 1 --input_format_json_read_arrays_as_strings 0)
echo '{"s":[1,"x"],"n":1.5}' | run "${OBJECTS[@]}" | check
