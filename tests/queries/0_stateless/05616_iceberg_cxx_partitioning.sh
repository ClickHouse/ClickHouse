#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Partitioned Iceberg tables written by Trino 408-480 for the tests of lithium-tech/iceberg-cxx:
# identity, bucket, year, month, day and hour transforms over all the primitive types, null partition
# values, partition evolution, and a table whose old snapshots were expired with `clean_expired_metadata`.
# The expected rows are the `INSERT` statements in the `create.sql` file next to each table, see
# `data_iceberg/iceberg_cxx/README.md`. Every filter on a partition column is checked with and without
# partition pruning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTRACTED=$(realpath -m "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_iceberg_cxx")

# clickhouse-local allows `icebergLocal` only for paths inside the current directory.
cd "$CUR_DIR/data_iceberg/iceberg_cxx" || exit 1

# Iceberg `time` is read as the number of microseconds since midnight: 61200000000 is 17:00:00.
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'no_partitioning';
SELECT * FROM icebergLocal('no_partitioning') ORDER BY ALL;

SELECT 'partitioned_table';
SELECT * FROM icebergLocal('partitioned_table') ORDER BY ALL;

SELECT 'partitioning_with_null_value';
SELECT * FROM icebergLocal('partitioning_with_null_value') ORDER BY ALL;

-- Identity partitioning by (c1, c2), then by c3 for the last insert.
SELECT 'partition_evolution';
SELECT * FROM icebergLocal('partition_evolution') ORDER BY ALL;

SELECT 'decimal_partitioning';
SELECT * FROM icebergLocal('decimal_partitioning') ORDER BY ALL;

SELECT 'bucket_partitioning';
SELECT * EXCEPT col_varbinary, hex(col_varbinary) FROM icebergLocal('bucket_partitioning') ORDER BY ALL;

SELECT 'year_date_partitioning';
SELECT * FROM icebergLocal('year_date_partitioning') ORDER BY ALL;
SELECT 'year_timestamp_partitioning';
SELECT * FROM icebergLocal('year_timestamp_partitioning') ORDER BY ALL;
SELECT 'year_timestamptz_partitioning';
SELECT * FROM icebergLocal('year_timestamptz_partitioning') ORDER BY ALL;
SELECT 'month_timestamptz_partitioning';
SELECT * FROM icebergLocal('month_timestamptz_partitioning') ORDER BY ALL;
SELECT 'day_timestamptz_partitioning';
SELECT * FROM icebergLocal('day_timestamptz_partitioning') ORDER BY ALL;
SELECT 'hour_timestamptz_partitioning';
SELECT * FROM icebergLocal('hour_timestamptz_partitioning') ORDER BY ALL;

-- Unpartitioned, then partitioned by day(c2); all rows deleted and inserted again, old snapshots expired.
SELECT 'w_clean_expired_metadata';
SELECT * FROM icebergLocal('w_clean_expired_metadata') ORDER BY ALL;
"

for pruning in 0 1
do
    echo "use_iceberg_partition_pruning = $pruning"
    $CLICKHOUSE_LOCAL --session_timezone UTC --use_iceberg_partition_pruning "$pruning" -q "
    SELECT 'year_date', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('year_date_partitioning') WHERE c2 >= '2025-01-01' ORDER BY c1);
    SELECT 'year_timestamp', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('year_timestamp_partitioning') WHERE c2 < '2025-01-01' ORDER BY c1);
    SELECT 'year_timestamptz', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('year_timestamptz_partitioning') WHERE c2 >= '2025-03-03 22:30:00' ORDER BY c1);
    SELECT 'month_timestamptz', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('month_timestamptz_partitioning') WHERE c2 < '2025-03-01' ORDER BY c1);
    SELECT 'day_timestamptz', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('day_timestamptz_partitioning') WHERE c2 IS NULL ORDER BY c1);
    SELECT 'hour_timestamptz', groupArray(c1) FROM (SELECT c1 FROM icebergLocal('hour_timestamptz_partitioning') WHERE c2 = '2025-03-03 23:00:00' ORDER BY c1);
    SELECT 'null_value', groupArray(c3) FROM (SELECT c3 FROM icebergLocal('partitioning_with_null_value') WHERE c1 IS NULL ORDER BY c3);
    SELECT 'evolution', groupArray(c3) FROM (SELECT c3 FROM icebergLocal('partition_evolution') WHERE c3 = 2 OR c1 = 2 ORDER BY c3);
    SELECT 'decimal', count() FROM icebergLocal('decimal_partitioning') WHERE col_decimal_9_2 = 3 AND col_decimal_18_2 = -5 AND col_decimal_19_2 = 0 AND col_decimal_38_2 = 0;
    SELECT 'bucket_int', count() FROM icebergLocal('bucket_partitioning') WHERE col_int = 1;
    SELECT 'bucket_int_other', count() FROM icebergLocal('bucket_partitioning') WHERE col_int = 2;
    SELECT 'bucket_long', count() FROM icebergLocal('bucket_partitioning') WHERE col_long = 2;
    SELECT 'bucket_decimal', count() FROM icebergLocal('bucket_partitioning') WHERE col_decimal = 3;
    SELECT 'bucket_date', count() FROM icebergLocal('bucket_partitioning') WHERE col_date = '2025-03-03';
    SELECT 'bucket_time', count() FROM icebergLocal('bucket_partitioning') WHERE col_time = 61200000000;
    SELECT 'bucket_timestamp', count() FROM icebergLocal('bucket_partitioning') WHERE col_timestamp = '2025-03-03 00:00:00';
    SELECT 'bucket_timestamptz', count() FROM icebergLocal('bucket_partitioning') WHERE col_timestamptz = '2025-03-03 00:00:00';
    SELECT 'bucket_string', count() FROM icebergLocal('bucket_partitioning') WHERE col_string = 'some-string';
    SELECT 'bucket_uuid', count() FROM icebergLocal('bucket_partitioning') WHERE col_uuid = '12151fd2-7586-11e9-8f9e-2a86e4085a59';
    SELECT 'bucket_varbinary', count() FROM icebergLocal('bucket_partitioning') WHERE col_varbinary = 'some-varbinary';
    "
done

# Identity partitioning by all 13 primitive columns. The partition directory names contain escaped
# characters (`col_time=17%3A00`), so the table is stored in a tar archive.
# The `uuid` partition value is read incorrectly: https://github.com/ClickHouse/ClickHouse/issues/123961
rm -rf "$EXTRACTED"
mkdir -p "$EXTRACTED"
tar -xf identity_partitioning.tar -C "$EXTRACTED"
cd "$EXTRACTED" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'identity_partitioning';
SELECT * EXCEPT col_varbinary, hex(col_varbinary) FROM icebergLocal('identity_partitioning') ORDER BY ALL;
SELECT count() FROM icebergLocal('identity_partitioning') WHERE col_uuid = '12151fd2-7586-11e9-8f9e-2a86e4085a59' AND col_string = 'some-string' AND col_time = 61200000000;
SELECT count() FROM icebergLocal('identity_partitioning') WHERE col_float = 2.25 AND col_double = 2.375 AND col_decimal = 3 AND col_timestamptz = '2025-03-03 00:00:00';
"

cd "$CUR_DIR" || exit 1
rm -rf "$EXTRACTED"
