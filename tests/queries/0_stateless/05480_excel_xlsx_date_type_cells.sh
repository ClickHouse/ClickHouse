#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cells of the date type (`t="d"`, the value is in the ISO 8601 format) are read as dates, like the dates stored as numbers.
# The `styled_date` column has a date number format, so its values with the time `00:00:00` are dates.
# The `unrepresentable` column has a date before 1900 and a time with a time zone offset, which are kept as strings.
FILE="$CUR_DIR/data_excel/date_type_cells.xlsx"

echo "--- schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE')"
echo "--- explicit structure"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', XLSX, 'date Date32, datetime DateTime64(3, \\'UTC\\'), time String, styled_date Date32, unrepresentable String')"
echo "--- everything as String"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', XLSX, 'date String, datetime String, time String, styled_date String, unrepresentable String')"
