#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading a subset of columns of `CustomSeparatedWithNames` skips the other fields,
# and `count()` skips whole rows. Check both for every escaping rule, with one-character
# and multi-character field delimiters.

function check()
{
    local rule=$1
    local delimiter=$2
    local values=$3
    local settings=(--format_custom_escaping_rule="$rule" --format_custom_field_delimiter="$delimiter")

    echo "$rule '$delimiter'"
    $CLICKHOUSE_LOCAL "${settings[@]}" -q "SELECT * FROM values('a String, b String, c UInt32', $values) FORMAT CustomSeparatedWithNames" > "$CLICKHOUSE_TMP/data"
    $CLICKHOUSE_LOCAL "${settings[@]}" --input-format CustomSeparatedWithNames --structure 'c UInt32' -q "SELECT c FROM table" < "$CLICKHOUSE_TMP/data"
    $CLICKHOUSE_LOCAL "${settings[@]}" --input-format CustomSeparatedWithNames --structure 'b String' -q "SELECT b FROM table" < "$CLICKHOUSE_TMP/data"
    $CLICKHOUSE_LOCAL "${settings[@]}" --input-format CustomSeparated --structure 'a String, b String, c UInt32' -q "SELECT count() FROM table" < "$CLICKHOUSE_TMP/data"
}

tricky="('x\ty\nz\\\\', 'q''t', 1), ('c,d', '\"e\"', 2), ('', '|', 3)"
simple="('xy', 'p q', 1), ('', 'r', 2), ('s', '', 3)"

check Escaped $'\t' "$tricky"
check Escaped $'\t|' "$tricky"
check Raw $'\t' "$simple"
check Quoted ';' "$tricky"
check Quoted '||' "$tricky"
check CSV ';' "$tricky"
check CSV '||' "$tricky"
check JSON ';' "$tricky"
check JSON '||' "$tricky"
