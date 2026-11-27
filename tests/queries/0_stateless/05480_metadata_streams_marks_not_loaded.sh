#!/usr/bin/env bash

# The prefixes read the metadata streams (the structures of JSON and Dynamic, the buckets of Map, the header of the
# SHARED paths of JSON) whole from their start, so a read loads no marks of them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TEST_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=test/g')

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_metadata_marks;
    CREATE TABLE t_metadata_marks (id UInt64, json JSON(SHARED s UInt64), d Dynamic, m Map(String, UInt64))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, prewarm_mark_cache = 0,
        object_typed_shared_data_serialization_version = 'v2',
        map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
    INSERT INTO t_metadata_marks
        SELECT number, toJSONString(map('a', number, 's', number)), number, map('k' || toString(number % 10), number)
        FROM numbers(1000);
"

# The log lines of the marks files a query loads.
function loaded_marks()
{
    $CLICKHOUSE_CLIENT_TEST_LOGS -q "$1 FORMAT Null" |& grep -o 'Loading marks from path .*'
}

whole=$(loaded_marks "SELECT * FROM t_metadata_marks")
subcolumns=$(loaded_marks "SELECT json.s, json.a, d.UInt64, m['k1'] FROM t_metadata_marks")

metadata_streams='\.(dynamic_structure|object_structure|buckets_info|header)\.'
data_marks=$(echo "$whole" | grep 'Loading marks' | grep -c -v -E "$metadata_streams")
echo "data marks loaded: $(( data_marks > 0 ))"
echo "metadata marks loaded: $(printf '%s\n%s\n' "$whole" "$subcolumns" | grep -c -E "$metadata_streams")"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_metadata_marks"
