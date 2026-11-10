#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Numbers in XLSX are stored as text, which is exact, while a double may be rounded (for integers above 2^53).
# Integers written with a decimal point or an exponent are converted to integers exactly,
# and numbers that are not integers are not silently rounded.
FILE="$CUR_DIR/data_excel/integral_number_text.xlsx"

echo "--- schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT exponent, unsigned FROM file('$FILE')"
echo "--- explicit structure"
$CLICKHOUSE_LOCAL -q "SELECT exponent, unsigned FROM file('$FILE', XLSX, 'exponent Int64, fraction Float64, unsigned UInt64')"
$CLICKHOUSE_LOCAL -q "SELECT exponent, unsigned FROM file('$FILE', XLSX, 'exponent Decimal(20, 0), fraction Float64, unsigned UInt128')"
echo "--- not an integer"
$CLICKHOUSE_LOCAL -q "SELECT fraction FROM file('$FILE', XLSX, 'exponent Int64, fraction Int64, unsigned UInt64')" 2>&1 | grep -o -m1 'UNEXPECTED_DATA_AFTER_PARSED_VALUE'
echo "--- round trip of a decimal with trailing zeros"
OUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"
$CLICKHOUSE_LOCAL -q "SELECT toDecimal128('9007199254740993.0', 1) AS x FORMAT XLSX SETTINGS output_format_decimal_trailing_zeros = 1" > "$OUT_FILE"
$CLICKHOUSE_LOCAL -q "SELECT x, toTypeName(x) FROM file('$OUT_FILE')"
$CLICKHOUSE_LOCAL -q "SELECT x FROM file('$OUT_FILE', XLSX, 'x UInt64')"
rm -f "$OUT_FILE"
