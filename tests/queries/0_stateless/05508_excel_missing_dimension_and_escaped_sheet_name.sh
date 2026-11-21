#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"

# sample_no_dimension.xlsx is sample.xlsx with the optional `<dimension>` elements removed from all worksheets.
# The `Second` sheet starts at B3, so the default range must start at column B in both files.
for file in sample.xlsx sample_no_dimension.xlsx
do
    echo "--- $file: inferred"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$CURDIR/data_excel/$file') SETTINGS input_format_excel_sheet = 'Second'"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CURDIR/data_excel/$file') SETTINGS input_format_excel_sheet = 'Second'"
    echo "--- $file: explicit structure"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CURDIR/data_excel/$file', XLSX, 'a UInt64, b String') SETTINGS input_format_excel_sheet = 'Second'"
done

echo "--- a sheet name with a literal _xHHHH_ sequence"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'Data_x0041_'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') SETTINGS input_format_excel_sheet = 'Data_x0041_'"

rm -f "$FILE"
