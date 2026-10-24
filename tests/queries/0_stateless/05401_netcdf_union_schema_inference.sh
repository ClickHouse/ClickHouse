#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_netcdf_union
mkdir -p "$DIR"

# Two files with different variables: the schema inferred in the `union` mode has both of them,
# and each file lacks one of its columns.
$CLICKHOUSE_LOCAL -q "SELECT number::Int32 AS a FROM numbers(3) INTO OUTFILE '$DIR/a.nc' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT (number * 10)::Int32 AS b FROM numbers(2) INTO OUTFILE '$DIR/b.nc' FORMAT NetCDF"

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DIR/{a,b}.nc', NetCDF) SETTINGS schema_inference_mode = 'union'"
$CLICKHOUSE_LOCAL -q "SELECT _file, * FROM file('$DIR/{a,b}.nc', NetCDF) ORDER BY _file, a, b SETTINGS schema_inference_mode = 'union'"

# An explicit structure with a column that the file does not have.
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DIR/a.nc', NetCDF, 'a Int32, c Nullable(String)') ORDER BY a"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DIR/a.nc', NetCDF, 'a Int32, c String') SETTINGS input_format_netcdf_allow_missing_columns = 0" 2>&1 | grep -o -m1 'THERE_IS_NO_COLUMN'

rm -rf "$DIR"
