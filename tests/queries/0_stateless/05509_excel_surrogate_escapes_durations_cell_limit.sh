#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_excel"
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"

echo "--- escaped UTF-16 surrogate pairs in a cell and in a sheet name"
# The text is `emoji _xD83D__xDE00_ and lone _xD83D_ end`, and the sheet name is `Smile _xD83D__xDE00_`.
$CLICKHOUSE_LOCAL -q "
    SELECT text, hex(text), isValidUTF8(text)
    FROM file('$DATA/escapes_and_durations.xlsx', XLSX, 'text String')
    WHERE text LIKE 'emoji%'
    SETTINGS input_format_excel_sheet = 'Smile 😀'"

echo "--- elapsed time formats keep whole days"
# h_mm_ss is the built-in format 46 (\`[h]:mm:ss\`), mm_ss is \`[mm]:ss\`, h_mm is \`[h]:mm\`, clock is a time of day (\`h:mm:ss\`).
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/escapes_and_durations.xlsx') SETTINGS input_format_excel_sheet = '1'"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/escapes_and_durations.xlsx') SETTINGS input_format_excel_sheet = '1'"
$CLICKHOUSE_LOCAL -q "SELECT h_mm_ss, mm_ss FROM file('$DATA/escapes_and_durations.xlsx', XLSX, 'text String, h_mm_ss Float64, mm_ss Float64') SETTINGS input_format_excel_sheet = '1'"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/durations.xls')"

echo "--- the length of a cell is limited to 32767 characters"
$CLICKHOUSE_LOCAL -q "SELECT repeat('x', 32767) AS s FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT length(s) FROM file('$FILE')"
$CLICKHOUSE_LOCAL -q "SELECT repeat('x', 32768) AS s FORMAT XLSX" 2>&1 > /dev/null | grep -o -m1 "too long for the XLSX format: 32768 characters"
# A character outside of the Basic Multilingual Plane counts as two.
$CLICKHOUSE_LOCAL -q "SELECT concat(repeat('x', 32765), '😀') AS s FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT lengthUTF8(s) FROM file('$FILE')"
$CLICKHOUSE_LOCAL -q "SELECT concat(repeat('x', 32766), '😀') AS s FORMAT XLSX" 2>&1 > /dev/null | grep -o -m1 "too long for the XLSX format: 32768 characters"

rm -f "$FILE"
