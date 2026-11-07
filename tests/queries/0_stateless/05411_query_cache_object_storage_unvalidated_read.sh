#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# The consistent query cache validates an object-storage read against the objects the read consumed,
# recording the `ETag` that the listing reported. With `s3_validate_etag_on_read = 0` the `GET` is not
# pinned to that `ETag`, so an overwrite between the listing and the `GET` would be read but certified
# with the old `ETag`. Such a read must not be cached: the second run is not a cache hit, unlike with
# the validation on.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

prefix="test_05411_${CLICKHOUSE_DATABASE}"

qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_s3_05411"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_05411 (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"

for validate in 0 0 1 1
do
    ${CLICKHOUSE_CLIENT} -q "SELECT sum(x), ${validate} AS validate FROM t_s3_05411 SETTINGS ${qc}, s3_validate_etag_on_read = ${validate}"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT sum(x), % FROM t_s3_05411%'
ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_s3_05411"
