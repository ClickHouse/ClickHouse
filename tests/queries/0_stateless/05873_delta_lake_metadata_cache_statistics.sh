#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOLDEN="$CUR_DIR/data_delta_lake/kernel_golden"
SCANNED="(SELECT sum(value) FROM system.events WHERE event = 'DeltaLakeScannedFiles')"

function check()
{
    local name=$1
    local table="deltaLakeLocal('$GOLDEN/$2/delta')"
    local condition=$3
    $CLICKHOUSE_LOCAL -q "
    SET use_delta_lake_metadata_cache = 1;
    SELECT '$name', count(), coalesce(sum(cityHash64(*)), 0) = (SELECT coalesce(sum(cityHash64(*)), 0) FROM $table WHERE $condition SETTINGS use_delta_lake_metadata_cache = 0)
    FROM $table WHERE $condition;
    CREATE TABLE scanned_before (value UInt64) ENGINE = Memory;
    INSERT INTO scanned_before SELECT $SCANNED;
    SELECT '$name warm', count() FROM $table WHERE $condition;
    SELECT '$name warm scanned files', $SCANNED - (SELECT value FROM scanned_before);
    "
}

check 'int lower' basic-with-inserts-updates 'id >= 50'
check 'int none' basic-with-inserts-updates 'id < 0'
check 'int or string' basic-with-inserts-updates "id >= 50 OR str = 'val=1'"
check 'two columns' data-skipping-partition-and-data-column 'part = 1 AND id = 1'
check 'either column' data-skipping-partition-and-data-column 'part = 1 OR id = 1'

check 'Int32' data-skipping-basic-stats-all-types 'as_int > 0'
check 'Int64' data-skipping-basic-stats-all-types 'as_long < 0'
check 'Int8' data-skipping-basic-stats-all-types 'as_byte != 0'
check 'Int16' data-skipping-basic-stats-all-types 'as_short > 0'
check 'Float32' data-skipping-basic-stats-all-types 'as_float > 0'
check 'Float64' data-skipping-basic-stats-all-types 'as_double < 0'
check 'Float64 negated' data-skipping-basic-stats-all-types 'NOT (as_double <= 0)'
check 'String' data-skipping-basic-stats-all-types "as_string > '0'"
check 'Date32' data-skipping-basic-stats-all-types "as_date > '2000-01-01'"
check 'DateTime64 above' data-skipping-basic-stats-all-types "as_timestamp > toDateTime64('2000-01-01 08:00:00.000999', 6, 'UTC')"
check 'DateTime64 within' data-skipping-basic-stats-all-types "as_timestamp >= toDateTime64('2000-01-01 08:00:00.000999', 6, 'UTC')"
check 'Decimal' data-skipping-basic-stats-all-types 'as_big_decimal > 0'
check 'column mapping' data-skipping-basic-stats-all-types-columnmapping-name 'as_int > 0'

check 'is null' data-reader-timestamp_ntz 'tsNtz IS NULL'
check 'is not null' data-reader-timestamp_ntz 'tsNtz IS NOT NULL'
check 'nullable greater' data-reader-timestamp_ntz "tsNtz > toDateTime64('2020-01-01 00:00:00', 6, 'UTC')"
check 'nullable greater or null' data-reader-timestamp_ntz "tsNtz > toDateTime64('2020-01-01 00:00:00', 6, 'UTC') OR tsNtz IS NULL"

check 'deletion vectors' dv-partitioned-with-checkpoint 'col1 > 30'
