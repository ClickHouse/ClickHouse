#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_null_map.nc

# Any nonzero byte of a null map is a NULL. Without short-circuit evaluation, `if` with a NULL
# branch uses its `UInt8` condition as the null map directly, so the NULL here is the byte 2.
$CLICKHOUSE_LOCAL -q "SELECT if(toUInt8(number * 2), NULL, toInt32(number)) AS x FROM numbers(2) INTO OUTFILE '$FILE' FORMAT NetCDF SETTINGS short_circuit_function_evaluation = 'disable'"
$CLICKHOUSE_LOCAL -q "SELECT x FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
