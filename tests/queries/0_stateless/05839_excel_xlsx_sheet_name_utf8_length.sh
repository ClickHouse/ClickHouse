#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"

# The length of a sheet name is validated with the same UTF-8 decoder as the one that writes it,
# so that the name in the file never has more than 31 characters.

A30='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'

echo "--- invalid UTF-8 is rejected instead of being written as replacement characters"
# 31 valid characters and a stray continuation byte: it would be written as a 32-character name.
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '${A30}a\\x80'" 2>&1 | grep -o -m1 "Invalid sheet name"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'a\\xC3'" 2>&1 | grep -o -m1 "Invalid sheet name"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '\\xED\\xA0\\x80'" 2>&1 | grep -o -m1 "Invalid sheet name"

echo "--- the boundary of 31 characters for multi-byte characters"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '${A30}é'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = '${A30}é'"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '${A30}éé'" 2>&1 | grep -o -m1 "Invalid sheet name"

echo "--- characters outside of the Basic Multilingual Plane count as two"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa😀'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa😀'"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '${A30}😀'" 2>&1 | grep -o -m1 "Invalid sheet name"

echo "--- the name of a continuation sheet is truncated by the same rules and does not split a character"
# 24 characters and three characters of two UTF-16 code units each: 30 code units in total, which do not fit with the suffix ` (2)`.
# The prefix of 27 code units ends after the first `😀` (26 code units), because the second one does not fit entirely.
NAME='aaaaaaaaaaaaaaaaaaaaaaaa😀😀😀'
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FROM numbers(1048576) FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '$NAME'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = '$NAME'"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = 'aaaaaaaaaaaaaaaaaaaaaaaa😀 (2)'"

rm -f "$FILE"
