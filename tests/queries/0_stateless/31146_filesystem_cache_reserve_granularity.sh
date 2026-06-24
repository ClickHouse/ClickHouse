#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: requires a cache disk.
# no-random-settings: randomized buffer sizes change the control-side reservation count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

set -e

# Populate a cache-on-write table and report:
#   1) the number of cache state-lock reservation attempts the INSERT made
#      (ProfileEvents['FilesystemCacheReserveAttempts']);
#   2) the number of resulting file segments in the cache.
# Echoes both numbers space-separated.
function run() {
    local granule=$1
    local suffix=$2

    local table="test_31146_${suffix}"
    local cache_path="31146_reserve_granularity_${CLICKHOUSE_DATABASE}_${suffix}"
    local qid="31146_${CLICKHOUSE_DATABASE}_${suffix}"

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $table (key UInt64, value String)
        ENGINE = MergeTree() ORDER BY key
        SETTINGS disk = disk(
            type = cache,
            name = '$cache_path',
            path = '$cache_path',
            disk = 'local_disk',
            max_size = '10Gi',
            max_file_segment_size = '1Mi',
            boundary_alignment = '1Mi',
            cache_on_write_operations = 1,
            reserve_granularity = '$granule'),
        min_bytes_for_wide_part = 0"

    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES $table"

    local cache_name
    cache_name=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.disks WHERE cache_path LIKE '%$cache_path%'")
    drop_filesystem_cache "$cache_name" > /dev/null

    # Incompressible value so the data actually spans many 1Mi file segments.
    $CLICKHOUSE_CLIENT --query_id "$qid" --enable_filesystem_cache_on_write_operations=1 -q "
        INSERT INTO $table SELECT number, randomString(1000) FROM numbers(20000)"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

    local attempts segments
    attempts=$($CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['FilesystemCacheReserveAttempts']
        FROM system.query_log
        WHERE query_id = '$qid' AND current_database = currentDatabase() AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1")
    segments=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'")

    echo "$attempts $segments"
}

read -r control_attempts control_segments < <(run 0 control)
read -r granule_attempts granule_segments < <(run '4Mi' granule)

# With reserve_granularity >= max_file_segment_size, each segment is reserved exactly once.
$CLICKHOUSE_CLIENT -q "SELECT 'one reserve per segment with granularity: ' || toString($granule_attempts = $granule_segments)"
# Disabling reserve-ahead reserves per write-chunk, i.e. at least once per segment.
$CLICKHOUSE_CLIENT -q "SELECT 'granularity does not increase attempts: ' || toString($granule_attempts <= $control_attempts)"
# The same number of segments is produced either way (the data is identical in size).
$CLICKHOUSE_CLIENT -q "SELECT 'same number of segments: ' || toString($granule_segments = $control_segments)"

# Data is intact in both cases.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(key) FROM test_31146_control"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(key) FROM test_31146_granule"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_31146_control"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_31146_granule"
