#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_null_as_default.nc

$CLICKHOUSE_LOCAL -q "SELECT CAST(arrayJoin([1, NULL, 3]) AS Nullable(Int32)) AS x INTO OUTFILE '$FILE' FORMAT NetCDF"

echo "--- input_format_null_as_default = 1"
$CLICKHOUSE_LOCAL -q "SELECT x FROM file('$FILE', NetCDF, 'x Int32') SETTINGS input_format_netcdf_fill_value_as_null = 1, input_format_null_as_default = 1"

echo "--- input_format_null_as_default = 0"
$CLICKHOUSE_LOCAL -q "SELECT x FROM file('$FILE', NetCDF, 'x Int32') SETTINGS input_format_netcdf_fill_value_as_null = 1, input_format_null_as_default = 0" 2>&1 | grep -o -m1 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN'

echo "--- a column with a DEFAULT expression"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (x Int32 DEFAULT 42) ENGINE = Memory;
    INSERT INTO t SELECT * FROM file('$FILE', NetCDF, 'x Nullable(Int32)') SETTINGS input_format_netcdf_fill_value_as_null = 1;
    INSERT INTO t FROM INFILE '$FILE' SETTINGS input_format_netcdf_fill_value_as_null = 1 FORMAT NetCDF;
    SELECT x FROM t;
"

rm -f "$FILE"
