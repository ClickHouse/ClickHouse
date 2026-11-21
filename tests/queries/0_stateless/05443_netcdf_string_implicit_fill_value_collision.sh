#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# Without the `_FillValue` attribute, `netCDF4` takes the default fill value of `char`, the zero
# byte, as missing data, and masks a zero byte inside a string. A string column without NULLs whose
# values contain a zero byte is written with an explicit `_FillValue` that the data does not
# contain, and the zero byte is read back as data.

echo "--- strings without NULLs"
$CLICKHOUSE_LOCAL -q "
    SELECT
        char(65, 0, 66) AS s,
        toNullable(char(65, 0, 66)) AS ns,
        toFixedString(char(0, 1), 2) AS fs,
        'AB' AS plain
    FROM numbers(2) INTO OUTFILE '$FILE' FORMAT NetCDF"

echo "--- the columns with a zero byte have a _FillValue and no NULLs, the others are not Nullable"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "
    SELECT hex(s), hex(ns), hex(fs), plain FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- every byte occurs: nothing is left, no attribute"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT concat(char(0), arrayStringConcat(arrayMap(x -> char(x), range(1, 256)))) AS s
    INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(s), length(s) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
