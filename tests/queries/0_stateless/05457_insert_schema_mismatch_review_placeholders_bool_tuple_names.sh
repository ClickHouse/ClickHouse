#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the structure-mismatch explanation of `INSERT` parse errors. In each case only
# the fractional `n` is invalid, so no explanation must be added, and the control without it inserts.

PHRASE="does not match the structure expected by the query"

check() {
    local output
    output=$(cat)
    grep -o -E "\((CANNOT_PARSE_[A-Z_]+|INCORRECT_DATA|TYPE_MISMATCH)\)" <<< "$output" | head -n1
    if grep -q "$PHRASE" <<< "$output"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    local format=$1 structure=$2 data=$3
    shift 3
    printf "$data" | $CLICKHOUSE_LOCAL "$@" --query "CREATE TABLE t ($structure) ENGINE = Memory; INSERT INTO t FORMAT $format" 2>&1 \
        && echo "inserted"
}

echo "-- An empty object into a JSON column is not evidence of a non-object token"
run JSONEachRow 'a JSON, n UInt8' '{"a":{},"n":1.5}\n' | check
run JSONEachRow 'a JSON, n UInt8' '{"a":{},"n":1}\n'
run JSONEachRow 'a JSON, n UInt8' '{"a":"text","n":1.5}\n' | check

echo "-- Custom boolean representations that are also numbers"
run TSV 'a UInt16, n UInt8' '1\t1.5\n' --bool_true_representation 1 --bool_false_representation 0 --input_format_tsv_detect_header 0 | check
run TSV 'a UInt16, n UInt8' '1\t1\n' --bool_true_representation 1 --bool_false_representation 0 --input_format_tsv_detect_header 0

echo "-- A tuple in one CSV field"
run CSV 'a Nullable(Tuple(UInt8, UInt8)), n UInt8' '"(1,2)",1.5\n' --input_format_csv_detect_header 0 | check
run CSV 'a Nullable(Tuple(UInt8, UInt8)), n UInt8' '"(1,2)",1\n' --input_format_csv_detect_header 0
run CSV 'a Tuple(UInt8, UInt8), n UInt8' '"(1,2)",1.5\n' --input_format_csv_detect_header 0 --input_format_csv_deserialize_separate_columns_into_tuple 0 | check
run CSV 'a Tuple(UInt8, UInt8), n UInt8' '"(1,2)",1\n' --input_format_csv_detect_header 0 --input_format_csv_deserialize_separate_columns_into_tuple 0

echo "-- A skipped unnamed column in CSVWithNamesAndTypes"
run CSVWithNamesAndTypes 'x UInt8, n UInt8' ',x,n\nString,UInt8,UInt8\nignored,1,1.5\n' --input_format_skip_unknown_fields 1 | check
run CSVWithNamesAndTypes 'x UInt8, n UInt8' ',x,n\nString,UInt8,UInt8\nignored,1,1\n' --input_format_skip_unknown_fields 1
run CSVWithNamesAndTypes 'x UInt8, n UInt8' ',x,n\nString,String,UInt8\nignored,1,1.5\n' --input_format_skip_unknown_fields 1 | check
