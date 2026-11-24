#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A single INT96 timestamp 1677-09-21 12:00:00 (julian day 2333836, time of day 43200000000000 ns), written by pyarrow
# with `use_deprecated_int96_timestamps=True`. Midnight of that day does not fit into Int64 nanoseconds, but the full value
# -9223329600000000000 does, so it must be read exactly instead of being saturated to the Int64 minimum or rejected.
DATA_FILE="${CUR_DIR}/data_parquet/05444_int96_lower_boundary.parquet"

${CLICKHOUSE_LOCAL} --query "
    SELECT id, ts, toUnixTimestamp64Nano(ts) FROM file('${DATA_FILE}', Parquet) SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT id, ts, toUnixTimestamp64Nano(ts) FROM file('${DATA_FILE}', Parquet) SETTINGS date_time_overflow_behavior = 'throw';"
