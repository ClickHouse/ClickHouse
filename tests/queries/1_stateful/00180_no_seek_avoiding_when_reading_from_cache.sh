#!/usr/bin/env bash

# Tags: no-parallel, no-random-settings, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Test assumes that the whole table is residing in the cache, but `hits_s3` has only 128Mi of cache.
# So we need to create a smaller table.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hits_s3_sampled"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hits_s3_sampled AS test.hits_s3"
$CLICKHOUSE_CLIENT -q "INSERT INTO hits_s3_sampled SELECT * FROM test.hits_s3 SAMPLE 0.01"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE hits_s3_sampled FINAL"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP FILESYSTEM CACHE"

# Warm up the cache
$CLICKHOUSE_CLIENT -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"
$CLICKHOUSE_CLIENT -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"

query_id=02906_read_from_cache_$RANDOM
$CLICKHOUSE_CLIENT --query_id ${query_id} -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"

$CLICKHOUSE_CLIENT -q "
  SYSTEM FLUSH LOGS;

  -- AsynchronousReaderIgnoredBytes = 0: no seek-avoiding happened
  -- CachedReadBufferReadFromSourceBytes = 0: sanity check to ensure we read only from cache
  SELECT ProfileEvents['AsynchronousReaderIgnoredBytes'], ProfileEvents['CachedReadBufferReadFromSourceBytes']
  FROM system.query_log
  WHERE query_id = '$query_id' AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hits_s3_sampled"
