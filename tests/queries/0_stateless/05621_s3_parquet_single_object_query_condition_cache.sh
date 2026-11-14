#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet and MinIO
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache, which a parallel test
# can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for a single Parquet object read from S3 by several sources: the entry is written once for
# the whole object, a split read uses it, and an object it fully prunes is skipped with one cache hit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"
OBJECT="${PREFIX}/05621_qcc.parquet"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${OBJECT}', ${AUTH}, 'Parquet')
    SELECT number AS id, toString(number % 1000) AS s, [number, number + 1] AS arr, (number, toString(number)) AS t
    FROM numbers(2000000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_use_custom_encoder = 1"

echo "-- the object is large enough to be split"
${CLICKHOUSE_CLIENT} --query "SELECT _size >= 8388608 FROM s3('${OBJECT}', ${AUTH}, 'One')"

function read_object()
{
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_05621_qcc_$1" --max_threads 4 --query "
        SELECT count(), sum(id) FROM s3('${OBJECT}', ${AUTH}, 'Parquet') PREWHERE id = $2
        SETTINGS use_query_condition_cache = 1, input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
            input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0,
            enable_parallel_replicas = 0, max_threads_min_free_memory_per_thread = 0"
}

echo "-- written once for the whole object, then used by a split read"
read_object 1 1234567
read_object 2 1234567
echo "-- no row group matches: written once, then the object is skipped"
read_object 3 2000001
read_object 4 2000001

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo "-- runs 1 and 2"
# The number of row groups depends on the block size, which is randomized.
${CLICKHOUSE_CLIENT} --query "
    WITH (SELECT num_row_groups FROM s3('${OBJECT}', ${AUTH}, 'ParquetMetadata')) AS row_groups
    SELECT
        ProfileEvents['QueryConditionCacheHits'],
        ProfileEvents['ObjectStorageQueryConditionCacheSkippedRowGroups'] = if(run = 2, row_groups - 1, 0),
        ProfileEvents['ParquetPrunedRowGroups'] = if(run = 2, row_groups - 1, 0),
        ProfileEvents['ObjectStorageReadObjects'] > 1
    FROM (SELECT ProfileEvents, toUInt8(right(query_id, 1)) AS run FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
            AND query_id IN ('${CLICKHOUSE_DATABASE}_05621_qcc_1', '${CLICKHOUSE_DATABASE}_05621_qcc_2'))
    ORDER BY run"
echo "-- runs 3 and 4"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['ObjectStorageQueryConditionCacheSkippedFiles'],
        ProfileEvents['ObjectStorageReadObjects'] > 1
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND query_id IN ('${CLICKHOUSE_DATABASE}_05621_qcc_3', '${CLICKHOUSE_DATABASE}_05621_qcc_4')
    ORDER BY query_id"
