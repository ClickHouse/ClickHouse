#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_excel"
# The extension `.zip` allows reading the parts of the written workbook with the archive syntax of the `file` function.
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.zip"

echo "--- cells with explicit references that are not ordered by column"
# The rows of the file list their cells as `F`, `B`, `D`; `D`, `F`, `B`; and `F`, `B`.
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/out_of_order_cells.xlsx') FORMAT TSVWithNames"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/out_of_order_cells.xlsx') SETTINGS input_format_excel_range = 'C1:F' FORMAT TSVWithNames"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/out_of_order_cells.xlsx') SETTINGS input_format_excel_range = 'A1:D' FORMAT TSVWithNames"

echo "--- control bytes in a string cell are written as escape sequences"
$CLICKHOUSE_LOCAL -q "SELECT concat('a', unhex('00'), 'b', unhex('01'), 'c', unhex('0B'), 'd', unhex('0C'), 'e', unhex('1F'), 'f') AS s FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT extract(raw_blob, '<t[^>]*>a[^<]*</t>') FROM file('$FILE :: xl/worksheets/sheet1.xml', RawBLOB)"
$CLICKHOUSE_LOCAL -q "SELECT hex(s) FROM file('$FILE', XLSX)"

echo "--- control bytes in a sheet name are written as escape sequences"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'A\x01B\x0BC\x0CD\x1FE'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT extract(raw_blob, '<sheet name=\"[^\"]*\"') FROM file('$FILE :: xl/workbook.xml', RawBLOB)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', XLSX) SETTINGS input_format_excel_sheet = 'A\x01B\x0BC\x0CD\x1FE'"

echo "--- no part of the written workbook has a raw control byte other than TAB, LF and CR"
for part in xl/worksheets/sheet1.xml xl/workbook.xml
do
    $CLICKHOUSE_LOCAL -q "SELECT match(raw_blob, '[\\\\x00-\\\\x08\\\\x0B\\\\x0C\\\\x0E-\\\\x1F]') FROM file('$FILE :: $part', RawBLOB)"
done

rm -f "$FILE"
