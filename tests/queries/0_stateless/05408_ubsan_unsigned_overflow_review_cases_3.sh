#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

echo "-- timeSlots stops after the last representable slot"
$CLICKHOUSE_LOCAL -q "SELECT arrayMap(x -> toUInt32(x), timeSlots(materialize(toDateTime(4294967295, 'UTC')), toUInt32(0), toUInt32(1)))"
$CLICKHOUSE_LOCAL -q "SELECT arrayMap(x -> toUInt32(x), timeSlots(toDateTime(4294967295, 'UTC'), materialize(toUInt32(0)), toUInt32(1)))"
$CLICKHOUSE_LOCAL -q "SELECT arrayMap(x -> toUInt32(x), timeSlots(materialize(toDateTime(4294967295, 'UTC')), materialize(toUInt32(0)), toUInt32(1)))"

echo "-- pointInPolygon with an empty hole"
$CLICKHOUSE_LOCAL -q "SELECT pointInPolygon((5, 5), [[(0, 0), (10, 0), (10, 10), (0, 10)], []]) SETTINGS validate_polygons = 0"

echo "-- Parquet with the maximum seek threshold"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$CUR_DIR/data_parquet/delta_length_byte_array_split_page.parquet', Parquet) SETTINGS input_format_parquet_local_file_min_bytes_for_seek = 18446744073709551615"

echo "-- unrepresentable free disk space to keep for a spill"
$CLICKHOUSE_LOCAL -q "
    SELECT number FROM numbers(3) ORDER BY number % 2
    SETTINGS max_threads = 1, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0,
        min_free_disk_space_for_temporary_data = 18446744073709551615
    FORMAT Null" 2>&1 | grep -o -m1 "NOT_ENOUGH_SPACE"
