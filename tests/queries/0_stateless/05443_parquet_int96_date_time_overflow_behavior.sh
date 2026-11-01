#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# INT96 timestamps 0001-01-01, 1800-01-01 12:30, 2020-05-17 10:20:30.123456, 9999-12-31 23:59:59 and NULL, written by
# pyarrow with `use_deprecated_int96_timestamps=True`. The first and the fourth do not fit into Int64 nanoseconds.
DATA_FILE="${CUR_DIR}/data_parquet/05443_int96_out_of_range.parquet"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', Parquet) ORDER BY id" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', Parquet) ORDER BY id SETTINGS date_time_overflow_behavior = 'throw'" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

${CLICKHOUSE_LOCAL} --query "
    SELECT id, ts, toTypeName(ts) FROM file('${DATA_FILE}', Parquet) ORDER BY id SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT id, ts, toTypeName(ts) FROM file('${DATA_FILE}', Parquet, 'id Int32, ts Nullable(DateTime64(6, \'UTC\'))') ORDER BY id SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE ts > '2000-01-01' SETTINGS date_time_overflow_behavior = 'saturate';"
