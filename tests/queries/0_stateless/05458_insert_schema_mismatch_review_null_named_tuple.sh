#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the structure-mismatch explanation of `INSERT` parse errors. In each case only
# an unrelated value is invalid, so no explanation must be added, and the control without it inserts.

PHRASE="does not match the structure expected by the query"

check() {
    local output
    output=$(cat)
    grep -o -E "\((CANNOT_PARSE_[A-Z_]+|CANNOT_READ_[A-Z_]+|INCORRECT_DATA|TYPE_MISMATCH)\)" <<< "$output" | head -n1
    if grep -q "$PHRASE" <<< "$output"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    local format=$1 structure=$2
    shift 2
    $CLICKHOUSE_LOCAL "$@" --query "CREATE TABLE t ($structure) ENGINE = Memory; INSERT INTO t FORMAT $format" 2>&1 \
        && echo "inserted"
}

echo "-- A JSON null inferred as a non-nullable String is an inference placeholder"
NULLABLE_0=(--schema_inference_make_columns_nullable 0 --input_format_null_as_default 1)
echo '{"a":null,"n":1.5}' | run JSONEachRow 'a Array(UInt8), n UInt8' "${NULLABLE_0[@]}" | check
echo '{"a":null,"n":1}' | run JSONEachRow 'a Array(UInt8), n UInt8' "${NULLABLE_0[@]}"
echo '{"a":"text","n":1.5}' | run JSONEachRow 'a Array(UInt8), n UInt8' "${NULLABLE_0[@]}" | check

echo "-- A named Tuple cast projects the elements by name"
native() { $CLICKHOUSE_LOCAL --query "SELECT $1 FORMAT Native"; }
CAST=(--input_format_native_allow_types_conversion 1)
native "(1, 2)::Tuple(x UInt8, y UInt8) AS a, 'not-a-uuid' AS u" | run Native 'a Tuple(x UInt8), u UUID' "${CAST[@]}" | check
native "(1, 2)::Tuple(x UInt8, y UInt8) AS a, '00000000-0000-0000-0000-000000000000' AS u" | run Native 'a Tuple(x UInt8), u UUID' "${CAST[@]}"
native "(1, 2)::Tuple(y UInt8, x UInt8) AS a, 'not-a-uuid' AS u" | run Native 'a Tuple(x UInt8, y UInt8), u UUID' "${CAST[@]}" | check
native "(2, 'abc')::Tuple(y UInt8, x String) AS a, 'not-a-uuid' AS u" | run Native 'a Tuple(x Array(UInt8)), u UUID' "${CAST[@]}" | check
