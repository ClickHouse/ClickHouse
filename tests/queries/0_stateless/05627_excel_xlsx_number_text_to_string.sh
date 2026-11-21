#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Numbers in XLSX are stored as text, which is exact, while a double may be rounded (for integers above 2^53).
# Integers read into `String` columns must be exact, both with an explicit structure and in an inferred `Nullable(String)` column.

echo "--- explicit String"
$CLICKHOUSE_LOCAL -q "SELECT exponent, fraction, unsigned FROM file('$CUR_DIR/data_excel/integral_number_text.xlsx', XLSX, 'exponent String, fraction String, unsigned String')"

FILE="$CUR_DIR/data_excel/mixed_number_text.xlsx"
echo "--- schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- inferred"
$CLICKHOUSE_LOCAL -q "SELECT mixed FROM file('$FILE')"
echo "--- explicit String"
$CLICKHOUSE_LOCAL -q "SELECT mixed FROM file('$FILE', XLSX, 'mixed String')"
