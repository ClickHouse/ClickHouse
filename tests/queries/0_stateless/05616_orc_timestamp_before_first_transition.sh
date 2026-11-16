#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no ORC format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Before the first transition of a time zone, the wall clock is kept with the oldest standard offset
# (Asia/Shanghai +08:00, America/Los_Angeles -08:00), not with the local mean time, as in other ORC readers.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.orc"

$CLICKHOUSE_LOCAL -q "
    SELECT arrayJoin([toDateTime64('1850-01-01 00:00:00', 0, 'UTC'), toDateTime64('1881-07-06 11:13:13', 0, 'UTC'), toDateTime64('1950-01-01 00:00:00', 0, 'UTC')]) AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')') SETTINGS input_format_orc_reader_time_zone_name = 'Asia/Shanghai'"

$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1850-01-01 00:00:00', 0, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'America/Los_Angeles'"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')')"

# Europe/Lisbon starts with a no-op transition record in 1884 that keeps the local mean time (-00:36:45).
$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1850-01-01 00:00:00', 0, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')') SETTINGS input_format_orc_reader_time_zone_name = 'Europe/Lisbon'"

# The timestamp statistics must agree with the values that are read, or the filter pushdown drops the row.
$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1850-01-01 00:00:00', 9, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'America/Los_Angeles'"
for push_down in 0 1; do
    $CLICKHOUSE_LOCAL -q "
        SELECT $push_down, ts FROM file('$FILE', ORC, 'ts DateTime64(9, \'UTC\')')
        WHERE ts = toDateTime64('1849-12-31 16:00:00', 9, 'UTC') SETTINGS input_format_orc_filter_push_down = $push_down"
done

rm -f "$FILE"
