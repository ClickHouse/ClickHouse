#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the structure-mismatch explanation of `INSERT` parse errors:
# - the -WithNamesAndTypes parser compares the declared type text before it is canonicalized, so a
#   declared alias (`Boolean`) for a `Bool` column is a mismatch;
# - a literal quote inside an unquoted `CSV` field does not close a later quoted multiline field, so
#   a streamed capture ending inside that field must not be sampled as a complete short row.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSVWithNamesAndTypes, declared alias"
for decl in Boolean UInt8; do
    printf "x\n$decl\n1\n" | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (x Bool) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes" 2>&1 | check
done
printf 'x\nBool\n1\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (x Bool) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes" 2>&1 && echo "inserted"

echo "-- A literal quote in an unquoted field before a streamed quoted multiline CSV field"
multiline() {
    { printf "$1"',"'; head -c 2097152 /dev/zero | tr '\0' '\n'; printf '",1\n'; } \
        | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 --input_format_csv_detect_header 0 \
            --query "CREATE TABLE t (n UInt8, s String, t String, k UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV" 2>&1 && echo "inserted"
}
multiline '1.5,a"b' | check
multiline '1,a"b'
multiline '1.5,ab' | check
printf '"text",a"b\n' | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 \
    --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV" 2>&1 | check
