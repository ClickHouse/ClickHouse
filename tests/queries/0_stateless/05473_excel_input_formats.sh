#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The data files were written by independent implementations:
# sample.xlsx and hidden_first.xlsx by openpyxl, sample.xls by xlwt (BIFF8),
# formulas.xlsx, formulas.xls and sample_libreoffice.xls by LibreOffice (with cached formula results),
# biff5.xls is a small Excel 5.0/95 (BIFF5) file stored in the mini stream of the compound file.
DATA="$CURDIR/data_excel"

function run()
{
    echo "--- $1"
    $CLICKHOUSE_LOCAL -q "$2"
}

for format in XLSX XLS; do
    if [ "$format" = XLSX ]; then file="$DATA/sample.xlsx"; else file="$DATA/sample.xls"; fi
    run "$format: schema inference" "DESCRIBE file('$file')"
    run "$format: data" "SELECT * FROM file('$file')"
    run "$format: sheet by name with an offset range" "SELECT * FROM file('$file') SETTINGS input_format_excel_sheet = 'Second'"
    run "$format: sheet by index" "SELECT * FROM file('$file') SETTINGS input_format_excel_sheet = '2'"
    run "$format: keep empty rows" "SELECT * FROM file('$file') SETTINGS input_format_excel_sheet = 'Second', input_format_excel_skip_empty_rows = 0"
    run "$format: no header" "SELECT * FROM file('$file') SETTINGS input_format_excel_sheet = 'Second', input_format_excel_header = 0"
    run "$format: range" "SELECT * FROM file('$file') SETTINGS input_format_excel_range = 'B2:C4', input_format_excel_header = 0"
    run "$format: open-ended range" "SELECT * FROM file('$file') SETTINGS input_format_excel_range = 'B4:C'"
    run "$format: explicit structure" "SELECT * FROM file('$file', $format, 'id UInt8, name String, price Decimal(30, 2), date Date32, ts DateTime(\\'UTC\\'), flag Nullable(Bool)') LIMIT 3"
    run "$format: everything as String" "SELECT * FROM file('$file', $format, 'id String, name String, price String, date String, ts String, flag String') ORDER BY id"
done

run "XLSX: numbers with a non-integer value in one column" "SELECT * FROM file('$DATA/sample.xlsx') SETTINGS input_format_excel_sheet = 'Numbers', input_format_excel_header = 0"
run "XLSX: the first visible sheet" "SELECT * FROM file('$DATA/hidden_first.xlsx') SETTINGS input_format_excel_header = 0"
run "XLSX: a hidden sheet by name" "SELECT * FROM file('$DATA/hidden_first.xlsx') SETTINGS input_format_excel_sheet = 'H', input_format_excel_header = 0"
run "XLSX: formulas" "SELECT * FROM file('$DATA/formulas.xlsx')"
run "XLS: formulas" "SELECT * FROM file('$DATA/formulas.xls')"
run "XLS: written by LibreOffice" "SELECT * FROM file('$DATA/sample_libreoffice.xls')"
run "XLS: many shared strings" "SELECT count(), sum(n), sum(length(s)), max(s) FROM file('$DATA/sample.xls') SETTINGS input_format_excel_sheet = 'Many'"
run "XLS: BIFF5" "DESCRIBE file('$DATA/biff5.xls')"
run "XLS: BIFF5 data" "SELECT * FROM file('$DATA/biff5.xls')"
run "XLS: time format" "SELECT time FROM file('$DATA/sample.xls')"
echo "--- XLSX: from stdin"
$CLICKHOUSE_LOCAL --input-format XLSX -q "SELECT id, name FROM table" < "$DATA/sample.xlsx"

echo "--- errors"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/sample.xlsx') SETTINGS input_format_excel_sheet = 'Missing'" 2>&1 | grep -o -m1 "does not have a sheet 'Missing'"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/sample.xlsx') SETTINGS input_format_excel_range = 'C3:A1'" 2>&1 | grep -o -m1 "Invalid cell range"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/sample.xlsx', XLSX, 'id Int64, name Int64')" 2>&1 | grep -o -m1 "while reading the column 'name'"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/sample.xls', XLSX)" 2>&1 | grep -o -m1 "CANNOT_UNPACK_ARCHIVE\|INCORRECT_DATA"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/sample.xlsx', XLS)" 2>&1 | grep -o -m1 "it does not have the signature of the Compound File Binary format"
