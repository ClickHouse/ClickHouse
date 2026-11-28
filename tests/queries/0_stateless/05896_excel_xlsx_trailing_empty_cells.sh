#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cells without a value that have only formatting (like `<c r="XFD1" s="1"/>`) at the right edge of a row do not add columns,
# but such cells between values are kept as empty cells. Row 4 has only such cells, so it is an empty row.
FILE="$CUR_DIR/data_excel/trailing_empty_cells.xlsx"

echo "--- schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE')"
echo "--- without a header"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE') SETTINGS input_format_excel_header = 0"
