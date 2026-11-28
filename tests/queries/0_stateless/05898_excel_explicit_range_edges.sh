#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An explicit range is a rectangle: with `input_format_excel_skip_empty_rows = 0` it keeps the empty rows at its top and bottom,
# and a range with the last column has all its columns, even if they are empty.
# The values of the sheet `Second` are in `B3:C6`, and the row 5 is empty.
DATA="$CUR_DIR/data_excel"

for file in "$DATA/sample.xlsx" "$DATA/sample.xls"; do
    format=$(basename "$file")
    format=${format##*.}
    format=${format^^}
    echo "--- $format: empty rows at the edges of the range"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM file('$file', $format, 'x Nullable(String), y Nullable(String)')
        SETTINGS input_format_excel_sheet = 'Second', input_format_excel_range = 'B1:C8',
            input_format_excel_skip_empty_rows = 0, input_format_excel_header = 0"
    echo "--- $format: empty rows are skipped by default"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM file('$file', $format, 'x Nullable(String), y Nullable(String)')
        SETTINGS input_format_excel_sheet = 'Second', input_format_excel_range = 'B1:C8', input_format_excel_header = 0"
    echo "--- $format: empty columns at the right of the range"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$file') SETTINGS input_format_excel_range = 'A:J'"
    echo "--- $format: an open range does not have empty columns at the right"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$file') SETTINGS input_format_excel_range = 'A1'"
done

echo "--- a range without values has all its columns in the empty rows"
for file in "$DATA/sample.xlsx" "$DATA/sample.xls"; do
    $CLICKHOUSE_LOCAL -q "
        DESCRIBE file('$file')
        SETTINGS input_format_excel_sheet = 'Second', input_format_excel_range = 'J1:K2',
            input_format_excel_skip_empty_rows = 0, input_format_excel_header = 0"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM file('$file')
        SETTINGS input_format_excel_sheet = 'Second', input_format_excel_range = 'J1:K2',
            input_format_excel_skip_empty_rows = 0, input_format_excel_header = 0"
done
