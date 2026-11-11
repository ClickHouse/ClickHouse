#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no ORC format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Before the first transition of a time zone, the wall clock is kept with the oldest standard offset
# (Asia/Shanghai +08:00 until 1901), not with the local mean time, as in other ORC readers.
# `DateTime64` starts at 1900 on this branch.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.orc"

$CLICKHOUSE_LOCAL -q "
    SELECT arrayJoin([toDateTime64('1900-06-01 00:00:00', 0, 'UTC'), toDateTime64('1900-07-06 11:13:13', 0, 'UTC'), toDateTime64('1950-01-01 00:00:00', 0, 'UTC')]) AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')') SETTINGS input_format_orc_reader_time_zone_name = 'Asia/Shanghai'"

$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1900-06-01 00:00:00', 0, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'Asia/Shanghai'"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')')"

# Europe/Lisbon starts with a no-op transition record in 1884 that keeps the local mean time (-00:36:45).
$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1900-06-01 00:00:00', 0, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')') SETTINGS input_format_orc_reader_time_zone_name = 'Europe/Lisbon'"

# Europe/London had +01:00 in 1970 and +00:00 in 1960, a lower offset before the epoch must not shift the day.
$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1960-01-15 00:00:00', 0, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'Europe/London'"
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(0, \'UTC\')')"

# The timestamp statistics must agree with the values that are read, or the filter pushdown drops the row.
$CLICKHOUSE_LOCAL -q "
    SELECT toDateTime64('1900-06-01 00:00:00', 9, 'UTC') AS ts
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'Asia/Shanghai'"
for push_down in 0 1; do
    $CLICKHOUSE_LOCAL -q "
        SELECT $push_down, ts FROM file('$FILE', ORC, 'ts DateTime64(9, \'UTC\')')
        WHERE ts = toDateTime64('1900-06-01 08:00:00', 9, 'UTC') SETTINGS input_format_orc_filter_push_down = $push_down"
done

# A time zone that `DateLUT` loads without an embedded file.
$CLICKHOUSE_LOCAL -q "SELECT ts FROM file('$FILE', ORC, 'ts DateTime64(9, \'UTC\')') SETTINGS input_format_orc_reader_time_zone_name = 'libc:UTC'"

rm -f "$FILE"
