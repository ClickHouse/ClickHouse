#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the structure-mismatch explanation of `INSERT` parse errors:
# - the -WithNamesAndTypes parser compares the declared type names, so a declared `Bool` for a
#   `UInt8` column is a mismatch even though the two types compare equal;
# - a `Template` row format mixing escaping rules must not invent a mismatch for a valid field;
# - a streamed capture ending with a line break inside a quoted multiline `CSV` field may end
#   inside a row, so it must not be sampled as a complete short row.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSVWithNamesAndTypes, declared type names"
for data in 'x\nBool\n1\n' 'x\nUInt16\n1\n' 'x\nUInt8\n1.5\n'; do
    printf "$data" | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes" 2>&1 | check
done

echo "-- Template with mixed escaping rules"
ROW_MIXED="$CLICKHOUSE_TMP/05449_row_mixed_$CLICKHOUSE_DATABASE.fmt"
printf '${b:Quoted}|${n:Escaped}' > "$ROW_MIXED"
printf "'true'|1.5" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_MIXED" \
    --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check
printf "'true'|1" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_MIXED" \
    --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 && echo "inserted"
rm -f "$ROW_MIXED"

echo "-- A streamed quoted multiline CSV field longer than the first captured buffer"
DATA_FILE="$CLICKHOUSE_TMP/05449_data_$CLICKHOUSE_DATABASE.csv"
multiline() {
    { printf "$1"',"'; head -c 2097152 /dev/zero | tr '\0' '\n'; printf '",1\n'; } > "$DATA_FILE"
    $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 --input_format_csv_detect_header 0 \
        --query "CREATE TABLE t (n UInt8, s String, k UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV" < "$DATA_FILE" 2>&1 && echo "inserted"
}
multiline '1.5' | check
multiline '1'
rm -f "$DATA_FILE"
printf '"text","hello"\n' | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 --query "
    CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT CSV
" 2>&1 | check
