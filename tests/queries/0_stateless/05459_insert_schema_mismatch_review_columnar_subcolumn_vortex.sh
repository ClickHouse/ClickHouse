#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest, no-msan: the `Arrow` and `Vortex` formats are not available in the fast-test and MSan builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the structure-mismatch explanation of `INSERT` parse errors with the columnar
# formats. In each case only an unrelated value is invalid, so no explanation must be added, and the
# control without it inserts.

PHRASE="does not match the structure expected by the query"

check() {
    local output
    output=$(cat)
    grep -o -E "\((CANNOT_PARSE_[A-Z_]+|CANNOT_READ_[A-Z_]+|INCORRECT_DATA|TYPE_MISMATCH|THERE_IS_NO_COLUMN)\)" <<< "$output" | head -n1
    if grep -q "$PHRASE" <<< "$output"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    local format=$1 structure=$2
    shift 2
    $CLICKHOUSE_LOCAL "$@" --query "CREATE TABLE t ($structure) ENGINE = Memory; INSERT INTO t FORMAT $format" 2>&1 \
        && echo "inserted"
}

generate() { $CLICKHOUSE_LOCAL --query "SELECT $2 FORMAT $1"; }
VALID_UUID="'00000000-0000-0000-0000-000000000000'"

echo "-- Arrow: a subcolumn of a struct is not a missing column"
NO_MISSING=(--input_format_arrow_allow_missing_columns 0)
generate Arrow "tuple(1)::Tuple(x UInt8) AS a, 'bad' AS u" | run Arrow '`a.x` UInt8, u UUID' "${NO_MISSING[@]}" | check
generate Arrow "tuple(1)::Tuple(x UInt8) AS a, $VALID_UUID AS u" | run Arrow '`a.x` UInt8, u UUID' "${NO_MISSING[@]}"

echo "-- Vortex: unknown columns are skipped and string columns are cast"
generate Vortex "'z' AS unused, 'bad' AS u" | run Vortex 'u UUID' | check
generate Vortex "'z' AS unused, $VALID_UUID AS u" | run Vortex 'u UUID'
generate Vortex "'[1,2]' AS a, 'bad' AS u" | run Vortex 'a Array(UInt8), u UUID' | check
generate Vortex "'[1,2]' AS a, $VALID_UUID AS u" | run Vortex 'a Array(UInt8), u UUID'
generate Vortex "'z' AS unused, 'abc' AS a, 'bad' AS u" | run Vortex 'a Array(UInt8), u UUID' | check
