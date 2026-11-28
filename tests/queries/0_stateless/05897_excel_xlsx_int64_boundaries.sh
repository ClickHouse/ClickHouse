#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Numbers at the boundaries of `Int64` are inferred as `Int64` and read exactly, although their doubles are rounded to 2^63.
# Numbers just outside the range are `Float64`. The `exponent` column has `9.223372036854775807E18` and `9.223372036854775808E18`.
FILE="$CUR_DIR/data_excel/int64_boundaries.xlsx"

echo "--- schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT max, min, toString(above_max), toString(below_min), toString(exponent) FROM file('$FILE')"
