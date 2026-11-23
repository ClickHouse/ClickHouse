#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reader keeps the wall clock in the reader time zone, while the statistics and the bloom filters are UTC
# instants. Filter push down must not change the result for any writer and reader time zone.

predicates=(
    "t = toDateTime64('2024-07-01 10:00:00', 9, 'UTC')"
    "t = toDateTime64('2024-07-01 12:00:00', 9, 'UTC')"
    "t >= toDateTime64('2024-07-02 08:00:00', 9, 'UTC')"
    "t < toDateTime64('2024-07-01 03:00:00', 9, 'UTC')"
    "t IN (toDateTime64('2024-07-01 10:00:00', 9, 'UTC'), toDateTime64('2024-07-02 12:00:00', 9, 'UTC'))"
    "n = toDateTime64('2024-07-01 11:00:00', 9, 'UTC')"
    "n IS NULL"
    "n IS NOT NULL AND t < toDateTime64('2024-07-01 12:00:00', 9, 'UTC')"
    "NOT (t >= toDateTime64('2024-07-01 02:00:00', 9, 'UTC'))"
)

writers=(GMT Europe/Berlin America/Los_Angeles)
readers=(GMT Europe/Berlin America/Los_Angeles Asia/Kolkata)

sql=""
for i in "${!writers[@]}"
do
    sql+="INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_$i.orc', ORC, 't DateTime64(3, \'UTC\'), n Nullable(DateTime64(3, \'UTC\'))')
        SELECT toDateTime64('2024-07-01 00:00:00', 3, 'UTC') + number * 3600, if(number % 5 = 0, NULL, toDateTime64('2024-07-01 00:00:00', 3, 'UTC') + number * 3600)
        FROM numbers(48)
        SETTINGS output_format_orc_writer_time_zone_name = '${writers[$i]}', output_format_orc_row_index_stride = 10,
            output_format_orc_compression_method = 'none', engine_file_truncate_on_insert = 1;
"
    for reader in "${readers[@]}"
    do
        sql+="SELECT '${writers[$i]}', '$reader', "
        for j in "${!predicates[@]}"
        do
            [ "$j" -gt 0 ] && sql+=", "
            read_file="file('${CLICKHOUSE_TEST_UNIQUE_NAME}_$i.orc', ORC, 't DateTime64(9, \'UTC\'), n Nullable(DateTime64(9, \'UTC\'))')"
            sql+="(SELECT count() FROM $read_file WHERE ${predicates[$j]} SETTINGS input_format_orc_filter_push_down = 1)
                = (SELECT count() FROM $read_file WHERE ${predicates[$j]} SETTINGS input_format_orc_filter_push_down = 0)"
        done
        sql+=" SETTINGS input_format_orc_reader_time_zone_name = '$reader';
"
    done
done

echo "$sql" | $CLICKHOUSE_CLIENT -n

for i in "${!writers[@]}"
do
    rm -f "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$i.orc"
done
