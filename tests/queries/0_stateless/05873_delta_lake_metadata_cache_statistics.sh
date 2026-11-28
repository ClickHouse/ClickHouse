#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake"
SCANNED="(SELECT sum(value) FROM system.events WHERE event = 'DeltaLakeScannedFiles')"

function check()
{
    local name=$1
    local table="deltaLakeLocal('$DATA/$2')"
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

check 'int lower' kernel_golden/basic-with-inserts-updates/delta 'id >= 50'
check 'int none' kernel_golden/basic-with-inserts-updates/delta 'id < 0'
check 'int or string' kernel_golden/basic-with-inserts-updates/delta "id >= 50 OR str = 'val=1'"
check 'two columns' kernel_golden/data-skipping-partition-and-data-column/delta 'part = 1 AND id = 1'
check 'either column' kernel_golden/data-skipping-partition-and-data-column/delta 'part = 1 OR id = 1'

check 'Int32' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_int > 0'
check 'Int64' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_long < 0'
check 'Int8' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_byte != 0'
check 'Int16' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_short > 0'
check 'Float32' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_float > 0'
check 'Float64' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_double < 0'
check 'Float64 negated' kernel_golden/data-skipping-basic-stats-all-types/delta 'NOT (as_double <= 0)'
check 'String' kernel_golden/data-skipping-basic-stats-all-types/delta "as_string > '0'"
check 'Date32' kernel_golden/data-skipping-basic-stats-all-types/delta "as_date > '2000-01-01'"
check 'DateTime64 above' kernel_golden/data-skipping-basic-stats-all-types/delta "as_timestamp > toDateTime64('2000-01-01 08:00:00.000999', 6, 'UTC')"
check 'DateTime64 within' kernel_golden/data-skipping-basic-stats-all-types/delta "as_timestamp >= toDateTime64('2000-01-01 08:00:00.000999', 6, 'UTC')"
check 'Decimal' kernel_golden/data-skipping-basic-stats-all-types/delta 'as_big_decimal > 0'
check 'column mapping' kernel_golden/data-skipping-basic-stats-all-types-columnmapping-name/delta 'as_int > 0'

check 'is null' kernel_golden/data-reader-timestamp_ntz/delta 'tsNtz IS NULL'
check 'is not null' kernel_golden/data-reader-timestamp_ntz/delta 'tsNtz IS NOT NULL'
check 'nullable greater' kernel_golden/data-reader-timestamp_ntz/delta "tsNtz > toDateTime64('2020-01-01 00:00:00', 6, 'UTC')"
check 'nullable greater or null' kernel_golden/data-reader-timestamp_ntz/delta "tsNtz > toDateTime64('2020-01-01 00:00:00', 6, 'UTC') OR tsNtz IS NULL"

check 'deletion vectors' kernel_golden/dv-partitioned-with-checkpoint/delta 'col1 > 30'

check 'statistics only in the checkpoint' kernel_data/v2-classic-parquet-struct-stats-only 'id = 3'
