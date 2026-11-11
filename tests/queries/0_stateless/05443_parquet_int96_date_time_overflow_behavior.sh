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

# `DateTime64` with a scale below 9 and `Date32` hold all four values, whatever the overflow behavior.
${CLICKHOUSE_LOCAL} --query "
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(DateTime64(6, \'UTC\'))');
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(DateTime64(6, \'UTC\'))') SETTINGS date_time_overflow_behavior = 'throw';
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(DateTime64(3, \'UTC\'))') SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(DateTime64(0, \'UTC\'))') SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(Date32)');
    SELECT arraySort(groupArray(ts)) FROM file('${DATA_FILE}', Parquet, 'ts Nullable(Date32)') SETTINGS date_time_overflow_behavior = 'saturate';"

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT
INT96_FILE="$TMP_DIR/int96.parquet"

# `frac`: -1 ns, -1500 ns and -0.500000001 s, rounded toward zero for `DateTime64` and down for `Date32`.
# `huge`: -300000 years, 0 and +300000 years, beyond the range of both `DateTime64` and `Date32`.
python3 - "$INT96_FILE" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

year = 365 * 86400
pq.write_table(pa.table({
    'frac': pa.array([-1, -1500, -500000001], pa.timestamp('ns')),
    'huge': pa.array([-300000 * year, 0, 300000 * year], pa.timestamp('s')),
}), sys.argv[1], use_deprecated_int96_timestamps=True)
PYEOF

${CLICKHOUSE_LOCAL} --query "
    SELECT arraySort(groupArray(toUnixTimestamp64Micro(frac))) FROM file('${INT96_FILE}', Parquet, 'frac DateTime64(6, \'UTC\')');
    SELECT arraySort(groupArray(toUnixTimestamp64Milli(frac))) FROM file('${INT96_FILE}', Parquet, 'frac DateTime64(3, \'UTC\')');
    SELECT arraySort(groupArray(toUnixTimestamp64Second(frac))) FROM file('${INT96_FILE}', Parquet, 'frac DateTime64(0, \'UTC\')');
    SELECT arraySort(groupArray(frac)) FROM file('${INT96_FILE}', Parquet, 'frac Date32');
    SELECT arraySort(groupArray(huge)) FROM file('${INT96_FILE}', Parquet, 'huge DateTime64(6, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
    SELECT arraySort(groupArray(huge)) FROM file('${INT96_FILE}', Parquet, 'huge Date32') SETTINGS date_time_overflow_behavior = 'saturate';"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${INT96_FILE}', Parquet, 'huge DateTime64(6, \'UTC\')')" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
