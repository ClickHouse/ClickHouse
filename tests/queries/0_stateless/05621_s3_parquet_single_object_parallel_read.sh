#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet, MinIO and Azurite

# A single Parquet object read from S3 or Azure is split between several sources, each reading a subset of its
# row groups, as a single local file is. The results are the same as with a single source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"
OBJECT="${PREFIX}/p=7/05621.parquet"
SMALL="${PREFIX}/05621_small.parquet"
MISSING="${PREFIX}/05621_missing.parquet"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${OBJECT}', ${AUTH}, 'Parquet')
    SELECT number AS id, toString(number % 1000) AS s, [number, number + 1] AS arr, (number, toString(number)) AS t
    FROM numbers(2000000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_use_custom_encoder = 1"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${SMALL}', ${AUTH}, 'Parquet')
    SELECT number AS id, toString(number) AS s FROM numbers(2000000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_use_custom_encoder = 1"

echo "-- the object is large enough to be split"
${CLICKHOUSE_CLIENT} --query "SELECT _size >= 8388608 FROM s3('${OBJECT}', ${AUTH}, 'One')"

function sources()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT sum(id) FROM s3('$1', ${AUTH}, 'Parquet') SETTINGS enable_parallel_replicas = 0, $2)
        WHERE trimLeft(explain) LIKE 'ReadFromObjectStorage%' OR trimLeft(explain) LIKE 'Resize 1 %'"
}

echo "-- split, one source per stream"
sources "${OBJECT}" "max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0"
echo "-- not split: order preserved, output not parallelized, no seeks, one thread"
sources "${OBJECT}" "max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 1"
sources "${OBJECT}" "max_threads = 4, parallelize_output_from_storages = 0"
sources "${OBJECT}" "max_threads = 4, input_format_parquet_preserve_order = 0, input_format_allow_seeks = 0"
sources "${OBJECT}" "max_threads = 1"
echo "-- not split: reads are not pinned to the listed ETag"
sources "${OBJECT}" "max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0, s3_validate_etag_on_read = 0"
echo "-- an object of two row groups: a source per stream, the split is decided while reading"
sources "${SMALL}" "max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0"
for threads in 32 1; do
    ${CLICKHOUSE_CLIENT} --max_threads "${threads}" --query "SELECT count(), sum(id), sum(length(s)) FROM s3('${SMALL}', ${AUTH}, 'Parquet')"
done

function read_object()
{
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT count(), sum(id), sum(length(s)), sum(arr[2]), sum(t.1), max(t.2) FROM s3('${OBJECT}', ${AUTH}, 'Parquet')"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT count(), sum(id) FROM s3('${OBJECT}', ${AUTH}, 'Parquet') WHERE s = '123'"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT count(), sum(id) FROM s3('${OBJECT}', ${AUTH}, 'Parquet') PREWHERE id % 7 = 0 WHERE s LIKE '1%'"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT count(), sum(id) FROM s3('${OBJECT}', ${AUTH}, 'Parquet') WHERE id BETWEEN 25500 AND 74321"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT sum(_row_number = id), sum(_row_number) FROM s3('${OBJECT}', ${AUTH}, 'Parquet')"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT id, s FROM s3('${OBJECT}', ${AUTH}, 'Parquet') ORDER BY id DESC LIMIT 3"
    ${CLICKHOUSE_CLIENT} "$@" --query "SELECT _file, _size > 0, count() FROM s3('${OBJECT}', ${AUTH}, 'Parquet') WHERE _path LIKE '%05621.parquet' GROUP BY ALL"
}

for threads in 8 1; do
    echo "-- max_threads = ${threads}"
    read_object --max_threads "${threads}"
done
# The sources of a split object that get no more than one parsing thread each parse in the threads of the pipeline.
echo "-- max_threads = 8, max_parsing_threads = 8"
read_object --max_threads 8 --max_parsing_threads 8

echo "-- hive partitioning columns"
${CLICKHOUSE_CLIENT} --query "SELECT p, count() FROM s3('${OBJECT}', ${AUTH}, 'Parquet') GROUP BY p SETTINGS use_hive_partitioning = 1, max_threads = 4"

echo "-- a filter on _path that needs a set built before reading"
${CLICKHOUSE_CLIENT} --max_threads 4 --query "
    SELECT count() FROM s3('${OBJECT}', ${AUTH}, 'Parquet') WHERE _path GLOBAL IN (SELECT _path FROM s3('${OBJECT}', ${AUTH}, 'One'))"

echo "-- a glob is not split"
${CLICKHOUSE_CLIENT} --max_threads 4 --query "SELECT count(), sum(id), sum(length(s)) FROM s3('${PREFIX}/p=7/0562?.parquet', ${AUTH}, 'Parquet')"
${CLICKHOUSE_CLIENT} --max_threads 4 --query "SELECT count(), sum(id), sum(length(s)) FROM s3('${OBJECT}', ${AUTH}, 'Parquet')"

echo "-- a missing object: building the pipeline does not read it, reading fails as without the split"
${CLICKHOUSE_CLIENT} --query "
    SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT sum(id) FROM s3('${MISSING}', ${AUTH}, 'Parquet', 'id UInt64')
        SETTINGS max_threads = 4, enable_parallel_replicas = 0)
    WHERE trimLeft(explain) LIKE 'ReadFromObjectStorage%'"
${CLICKHOUSE_CLIENT} --max_threads 4 --query "SELECT sum(id) FROM s3('${MISSING}', ${AUTH}, 'Parquet', 'id UInt64')" 2>&1 | grep -o -m1 'S3_ERROR'
${CLICKHOUSE_CLIENT} --max_threads 4 --query "SELECT sum(id) FROM s3('${MISSING}', ${AUTH}, 'Parquet', 'id UInt64') SETTINGS s3_ignore_file_doesnt_exist = 1"

echo "-- query condition cache: written once for the whole object, then used by a split read"
for i in 1 2; do
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_05621_qcc_${i}" --max_threads 4 --query "
        SELECT count(), sum(id) FROM s3('${OBJECT}', ${AUTH}, 'Parquet') PREWHERE id = 1234567
        SETTINGS use_query_condition_cache = 1, input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
            input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0"
done
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# The number of row groups depends on the block size, which is randomized.
${CLICKHOUSE_CLIENT} --query "
    WITH (SELECT num_row_groups FROM s3('${OBJECT}', ${AUTH}, 'ParquetMetadata')) AS row_groups
    SELECT
        ProfileEvents['QueryConditionCacheHits'],
        ProfileEvents['ObjectStorageQueryConditionCacheSkippedRowGroups'] = if(run = 2, row_groups - 1, 0),
        ProfileEvents['ParquetPrunedRowGroups'] = if(run = 2, row_groups - 1, 0)
    FROM (SELECT ProfileEvents, toUInt8(right(query_id, 1)) AS run FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
            AND query_id LIKE '${CLICKHOUSE_DATABASE}_05621_qcc_%')
    ORDER BY run"

echo "-- Azure"
AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"
AZURE_OBJECT="azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', '05621.parquet', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${AZURE_OBJECT}
    SELECT number AS id, toString(number % 1000) AS s, [number, number + 1] AS arr FROM numbers(2000000)
    SETTINGS azure_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_use_custom_encoder = 1"
${CLICKHOUSE_CLIENT} --query "SELECT _size >= 8388608 FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', '05621.parquet', 'One')"
for etag in 1 0; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT sum(id) FROM ${AZURE_OBJECT}
            SETTINGS max_threads = 4, enable_parallel_replicas = 0, azure_validate_etag_on_read = ${etag})
        WHERE trimLeft(explain) LIKE 'ReadFromObjectStorage%' OR trimLeft(explain) LIKE 'Resize 1 %'"
done
for threads in 8 1; do
    ${CLICKHOUSE_CLIENT} --max_threads "${threads}" --query "SELECT count(), sum(id), sum(arr[2]) FROM ${AZURE_OBJECT}"
done
