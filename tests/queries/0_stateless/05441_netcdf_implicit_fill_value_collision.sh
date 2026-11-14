#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# Without the `_FillValue` attribute, the readers of the format (the netCDF library, `netCDF4`)
# take the default fill value of the type as missing data. The conventions exempt the types of one
# byte, but `netCDF4` masks them in the classic formats too, so they are handled the same way. A
# column without NULLs whose data contains that value is written with an explicit `_FillValue` that
# the data does not contain, and the value is read back as a value, not as a NULL.

echo "--- the default fill values in columns without NULLs"
$CLICKHOUSE_LOCAL -q "
    SELECT
        toInt16(-32767) AS i16,
        toUInt16(65535) AS u16,
        toInt32(-2147483647) AS i32,
        toUInt32(4294967295) AS u32,
        toInt64(-9223372036854775806) AS i64,
        toUInt64(18446744073709551614) AS u64,
        toFloat32(9.9692099683868690e+36) AS f32,
        toFloat64(9.9692099683868690e+36) AS f64,
        toNullable(toInt32(-2147483647)) AS ni32,
        toInt32(number) AS plain,
        toInt8(-127) AS i8,
        toUInt8(255) AS u8
    FROM numbers(2) INTO OUTFILE '$FILE' FORMAT NetCDF"

echo "--- the columns that collide have a _FillValue and no NULLs, the others are not Nullable"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- without the setting"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

echo "--- UInt16 taking every value: nothing is left, no attribute"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT toUInt16(number) AS x FROM numbers(65536) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(x), count(), uniqExact(x) FROM file('$FILE', NetCDF)
    GROUP BY 1 SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- UInt8 taking every value: nothing is left, no attribute"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT toUInt8(number) AS x FROM numbers(256) INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(x), count(), uniqExact(x) FROM file('$FILE', NetCDF)
    GROUP BY 1 SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
