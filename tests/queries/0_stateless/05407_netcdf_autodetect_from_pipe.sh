#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The format of the data from a pipe is detected by probing the formats one after another on a
# buffer that is rolled back between them, so the count of the buffer includes the bytes read by
# the earlier probes. The header of the NetCDF file must be measured from its own beginning.
$CLICKHOUSE_LOCAL --input-format auto -q 'SELECT * FROM table LIMIT 1' < "$CUR_DIR/data_netcdf/example_cdf1.nc"
