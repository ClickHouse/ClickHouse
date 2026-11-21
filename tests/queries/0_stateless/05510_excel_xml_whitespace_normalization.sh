#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_excel"
# The extension `.zip` allows reading the parts of the written workbook with the archive syntax of the `file` function.
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.zip"

echo "--- literal CR and CRLF in the XML become LF, character references and escapes are preserved"
# The XML of the file has literal CR and CRLF in a shared string, an inline string, a cached formula result, a CDATA section,
# and in rich text runs. The sheet name attribute is `A<TAB>B<CR><LF>C&#10;D`, which XML parsers read as `A B C\nD`.
# The value `long` is `a\r\nbb\r\nccc\r\n` repeated 30000 times.
$CLICKHOUSE_LOCAL -q "
    SELECT name, if(length(value) < 100, hex(value), concat(toString(length(value)), ' ', toString(countSubstrings(value, '\r'))))
    FROM file('$DATA/xml_line_endings.xlsx')
    SETTINGS input_format_excel_sheet = 'A B C\nD'"

echo "--- whitespace in a sheet name is written as character references"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'Data\tOne\nTwo\rThree'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT extract(raw_blob, '<sheet name=\"[^\"]*\"') FROM file('$FILE :: xl/workbook.xml', RawBLOB)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', XLSX) SETTINGS input_format_excel_sheet = 'Data\tOne\nTwo\rThree'"

echo "--- the length of a cell counts every invalid UTF-8 byte as a replacement character"
$CLICKHOUSE_LOCAL -q "SELECT concat(repeat('x', 32766), unhex('80')) AS s FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT lengthUTF8(s), hex(substring(s, 32767)) FROM file('$FILE', XLSX)"
$CLICKHOUSE_LOCAL -q "SELECT concat(repeat('x', 32767), unhex('80')) AS s FORMAT XLSX" 2>&1 > /dev/null | grep -o -m1 "too long for the XLSX format: 32768 characters"
$CLICKHOUSE_LOCAL -q "SELECT concat(repeat('x', 32766), unhex('F09F98')) AS s FORMAT XLSX" 2>&1 > /dev/null | grep -o -m1 "too long for the XLSX format: 32769 characters"

rm -f "$FILE"
