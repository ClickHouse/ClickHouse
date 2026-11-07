#!/usr/bin/env bash
# Tags: no-fasttest

# Partitioned Iceberg tables from the Apache Doris tests, see `data_iceberg/doris/README.md`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/iceberg_partition_upper_case_nereids.out`,
# `test_external_catalog_iceberg_partition.out` and `test_iceberg_statistics.out`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# Identity partition on a column named `City` (upper case) or `city` (lower case); the other columns are `K1`, `K2`, `K3`.
for table in iceberg_partition_upper_case_orc iceberg_partition_upper_case_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY K1;
        SELECT K1, City FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY K1;
        SELECT City FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY City;
        SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE K1 > 1 AND City = 'Beijing' ORDER BY K1;
    " 2>&1
done
for table in iceberg_partition_lower_case_orc iceberg_partition_lower_case_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY K1;
        SELECT K1, city FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY K1;
        SELECT city FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY city;
        SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE K1 > 1 AND city = 'Beijing' ORDER BY K1;
    " 2>&1
done

# Identity partitions on an int column with NULL values (`one_column`), on int, float and string columns (`columns`),
# and identity plus `truncate[1]` (`truncate_and_fields`). `t_timestamp` is a `timestamptz`, shown in the `Asia/Shanghai`
# time zone as in the Doris test.
for table in parquet_partitioned_one_column parquet_partitioned_columns parquet_partitioned_truncate_and_fields \
             orc_partitioned_one_column orc_partitioned_columns orc_partitioned_truncate_and_fields
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT t_int, t_float, t_string, toTimeZone(t_timestamp, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY t_float;
        SELECT t_int, t_float, t_string FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE t_int IS NULL ORDER BY t_float;
        SELECT t_int, t_float, t_string FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE t_int IS NOT NULL ORDER BY t_float;
        SELECT t_int, t_float, t_string FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE t_string = 'test2' ORDER BY t_float;
        SELECT t_int, t_float, t_string FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE t_string LIKE 't%' AND t_float > 0.15 ORDER BY t_float;
    " 2>&1
done

# A date column without a partition, with an identity partition and with a `day` partition.
echo "user_case_date"
$CLICKHOUSE_LOCAL -q "
    SELECT id, b, d, toTimeZone(ts, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE d = '2020-01-02';
    SELECT id, b, d, toTimeZone(ts, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE d > '2020-01-01';
    SELECT id, b, d, toTimeZone(ts, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE d < '2020-01-03';
    SELECT id, b, d, toTimeZone(ts, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE ts < toDateTime64('2020-01-03 00:00:00', 6, 'Asia/Shanghai');
    SELECT id, b, d, toTimeZone(ts, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE ts > toDateTime64('2020-01-01 00:00:00', 6, 'Asia/Shanghai');
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_without_partition') WHERE ts > toDateTime64('2020-01-02 01:02:03.123456', 6, 'Asia/Shanghai');
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_date_partition') WHERE d = '2020-01-02';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_date_partition') WHERE d < '2020-01-03';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_date_partition') WHERE d > '2020-01-01';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_date_partition') WHERE d > '2020-01-02';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_days_date_partition') WHERE d = '2020-01-02';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_days_date_partition') WHERE d < '2020-01-03';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_days_date_partition') WHERE d > '2020-01-01';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/user_case_date_with_days_date_partition') WHERE d < '2020-01-02';
" 2>&1

# 1000 rows of all primitive types, partitioned by `day(col_timestamp)`, `identity(city)` and `bucket[2](id)`.
# `format_v1/sample_orc` is a format version 1 ORC table, `format_v2/sample_mor_parquet` is a format version 2 Parquet table
# where 11 rows were updated with merge-on-read (position deletes), so it has 1011 rows in data files and 11 position deletes.
# Doris computes the column statistics of the table (count, number of NULLs, min, max).
for table in format_v1/sample_orc format_v2/sample_mor_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT arrayJoin([
            ('city', count(city), countIf(city IS NULL), toString(min(city)), toString(max(city))),
            ('col_boolean', count(col_boolean), countIf(col_boolean IS NULL), toString(min(col_boolean)), toString(max(col_boolean))),
            ('col_byte', count(col_byte), countIf(col_byte IS NULL), toString(min(col_byte)), toString(max(col_byte))),
            ('col_char', count(col_char), countIf(col_char IS NULL), toString(min(col_char)), toString(max(col_char))),
            ('col_date', count(col_date), countIf(col_date IS NULL), toString(min(col_date)), toString(max(col_date))),
            ('col_decimal', count(col_decimal), countIf(col_decimal IS NULL), toString(min(col_decimal)), toString(max(col_decimal))),
            ('col_double', count(col_double), countIf(col_double IS NULL), toString(min(col_double)), toString(max(col_double))),
            ('col_float', count(col_float), countIf(col_float IS NULL), toString(min(col_float)), toString(max(col_float))),
            ('col_integer', count(col_integer), countIf(col_integer IS NULL), toString(min(col_integer)), toString(max(col_integer))),
            ('col_long', count(col_long), countIf(col_long IS NULL), toString(min(col_long)), toString(max(col_long))),
            ('col_short', count(col_short), countIf(col_short IS NULL), toString(min(col_short)), toString(max(col_short))),
            ('col_string', count(col_string), countIf(col_string IS NULL), toString(min(col_string)), toString(max(col_string))),
            ('col_timestamp', count(col_timestamp), countIf(col_timestamp IS NULL), toString(toTimeZone(min(col_timestamp), 'Asia/Shanghai')), toString(toTimeZone(max(col_timestamp), 'Asia/Shanghai'))),
            ('col_timestamp_ntz', count(col_timestamp_ntz), countIf(col_timestamp_ntz IS NULL), toString(min(col_timestamp_ntz)), toString(max(col_timestamp_ntz))),
            ('col_varchar', count(col_varchar), countIf(col_varchar IS NULL), toString(min(col_varchar)), toString(max(col_varchar))),
            ('id', count(id), countIf(id IS NULL), toString(min(id)), toString(max(id)))]) AS stats
        FROM icebergLocal('warehouse/$table')
        SETTINGS session_timezone = 'UTC';

        -- Partition pruning must not change the results.
        SELECT city, toDate(col_timestamp), count(), sum(id), sum(col_integer) FROM icebergLocal('warehouse/$table') GROUP BY ALL ORDER BY ALL;
        SELECT count(), sum(id) FROM icebergLocal('warehouse/$table') WHERE city = 'Beijing';
        SELECT count(), sum(id) FROM icebergLocal('warehouse/$table') WHERE col_timestamp >= '1970-01-03 00:00:00' AND city != 'Shanghai';
        SELECT count(), sum(id) FROM icebergLocal('warehouse/$table') WHERE id IN (-99567408, 99854631);
    " 2>&1
done
