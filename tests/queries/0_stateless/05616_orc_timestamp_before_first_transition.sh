#!/usr/bin/env bash

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

rm -f "$FILE"
