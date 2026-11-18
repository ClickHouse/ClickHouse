#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the time zone files are not embedded in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ORC filter push down prunes TIMESTAMP row groups when the writer and reader time zones are UTC under any name.
# `indexHint` keeps every row of the row groups that are read, so `count` is the number of rows read.

file="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
for writer in UTC Etc/UTC Universal
do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('$file', ORC, 't DateTime64(9, \'UTC\')')
        SELECT toDateTime64('2024-01-01 00:00:00', 9, 'UTC') + toIntervalSecond(number) FROM numbers(10000)
        SETTINGS output_format_orc_writer_time_zone_name = '$writer', output_format_orc_row_index_stride = 1000, engine_file_truncate_on_insert = 1"
    for reader in GMT Etc/UTC Zulu Europe/Berlin
    do
        $CLICKHOUSE_CLIENT -q "
            SELECT '$writer', '$reader', count() FROM file('$file', ORC, 't DateTime64(9, \'UTC\')')
            WHERE indexHint(t = toDateTime64('2024-01-01 00:00:05', 9, 'UTC'))
            SETTINGS input_format_orc_filter_push_down = 1, input_format_orc_reader_time_zone_name = '$reader'"
    done
done

# A zone with a fixed offset since 1895 keeps the wall clock of its older offsets.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$file', ORC, 't DateTime64(0, \'UTC\')') SELECT toDateTime64('1885-01-01 00:00:00', 0, 'UTC')
    SETTINGS output_format_orc_writer_time_zone_name = 'Pacific/Port_Moresby', engine_file_truncate_on_insert = 1"
for reader in Pacific/Port_Moresby Etc/GMT-10
do
    $CLICKHOUSE_CLIENT -q "
        SELECT '$reader', t FROM file('$file', ORC, 't DateTime64(0, \'UTC\')')
        SETTINGS input_format_orc_reader_time_zone_name = '$reader'"
done

rm -f "${USER_FILES_PATH:?}/$file"
