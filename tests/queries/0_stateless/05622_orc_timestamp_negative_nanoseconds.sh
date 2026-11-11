#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no ORC format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_orc"
WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_orc_timestamp_negative_nanoseconds"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

VALUES="
    SELECT arrayJoin([
        '1969-12-31 23:59:58.5', '1969-12-31 23:59:58.999999999', '1969-12-31 23:59:59.000000001',
        '1969-12-31 23:59:58.000999', '1960-06-15 12:34:56.789', '1900-01-01 00:00:00.000001',
        '1969-12-31 23:59:59', '1970-01-01 00:00:00', '1970-01-01 00:00:00.5', '2024-07-01 12:00:00.123456'])"

echo "-- the nanoseconds of the timestamps before the epoch are not negative"
for scale in 1 3 6 9; do
    ${CLICKHOUSE_LOCAL} -q "SELECT toDateTime64(s, $scale, 'UTC') AS t FROM ($VALUES AS s) INTO OUTFILE '$WORK_DIR/t$scale.orc' TRUNCATE FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
    # A negative number in the unsigned nanoseconds stream is stored as a run of 0xFF bytes.
    ${CLICKHOUSE_LOCAL} -q "SELECT $scale AS scale, position(raw, unhex('FFFFFFFFFFFF')) = 0 AS no_negative_nanoseconds FROM file('$WORK_DIR/t$scale.orc', RawBLOB, 'raw String')"
    ${CLICKHOUSE_LOCAL} -q "SELECT groupArray(t) FROM file('$WORK_DIR/t$scale.orc', ORC, 't DateTime64($scale, \\'UTC\\')')"
done

echo "-- the timestamps between -1 and 0 seconds keep negative nanoseconds"
${CLICKHOUSE_LOCAL} -q "
    SELECT toDateTime64(s, 9, 'UTC') AS t
    FROM (SELECT arrayJoin(['1969-12-31 23:59:59.5', '1969-12-31 23:59:59.999999999', '1969-12-31 23:59:59.001']) AS s)
    INTO OUTFILE '$WORK_DIR/between.orc' TRUNCATE FORMAT ORC"
${CLICKHOUSE_LOCAL} -q "SELECT groupArray(t) FROM file('$WORK_DIR/between.orc', ORC, 't DateTime64(9, \\'UTC\\')')"

echo "-- the files written with negative nanoseconds are read correctly"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$DATA_DIR/negative_nanoseconds_legacy.orc') SETTINGS session_timezone = 'UTC'"
