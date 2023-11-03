#!/usr/bin/env bash

# Tags: no-parallel, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Warm up the cache
$CLICKHOUSE_CLIENT -q "SELECT * FROM test.hits_s3 WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test.hits_s3 WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null"

query_id=02906_read_from_cache_$RANDOM
$CLICKHOUSE_CLIENT --query_id ${query_id} -q "SELECT * FROM test.hits_s3 WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null"

$CLICKHOUSE_CLIENT -nq "
  SYSTEM FLUSH LOGS;
  SELECT ProfileEvents['AsynchronousReaderIgnoredBytes'] FROM system.query_log WHERE query_id = '$query_id' AND type = 'QueryFinish'
"
