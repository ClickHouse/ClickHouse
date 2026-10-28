#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: Iceberg needs the Avro and Parquet libraries and MinIO
# Tag no-parallel-replicas: counts the S3 requests of a query, and under parallel replicas they are made by other replicas

# Reading an Iceberg table makes no object-metadata request (S3 HEAD) for its manifests and, through a table,
# for its Parquet data files: they are never overwritten, and the data file sizes are recorded in the table
# metadata. Avro data files (`t2`) keep the request. Each INSERT adds one data file and one manifest, so the
# requests of a read are compared between 3 and 6 files.
# t3 is a fixture whose data file records a negative size, which keeps the request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_TEST_UNIQUE_NAME}/t0"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}_"

# The Iceberg metadata files cache is chosen when the table is loaded: `t0` reads its manifests from S3,
# `t1` reads the same files with the cache.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 --query "CREATE TABLE t0 (c0 Int32, c1 String) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 1 --query "CREATE TABLE t1 ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 --query "CREATE TABLE t2 (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/t2', format = 'Avro')"

insert()
{
    for i in "$@"; do
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 \
            --query "INSERT INTO t0 VALUES ($i, toString($i))"
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 \
            --query "INSERT INTO t2 VALUES ($i)"
    done
}

LAZY="SELECT c1 FROM t0 ORDER BY c0 LIMIT 100"
LAZY_SETTINGS="--query_plan_optimize_lazy_materialization 1 --query_plan_optimize_lazy_materialization_for_object_storage 1 --query_plan_max_limit_for_lazy_materialization 0"

read_all()
{
    local settings="--use_iceberg_metadata_files_cache 0 --s3_validate_etag_on_read 1"
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}a_$1" --query "SELECT sum(c0) FROM t0"
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}b_$1" --query "SELECT sum(c0) FROM icebergS3(s3_conn, filename = '${TABLE_PATH}')"
    ${CLICKHOUSE_CLIENT} $settings $LAZY_SETTINGS --query_id "${Q}c_$1" --query "$LAZY" | xargs
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}d_$1" --query "SELECT sum(c0) FROM t2"
}

insert 1 2 3
read_all 3
insert 4 5 6
read_all 6

${CLICKHOUSE_CLIENT} $LAZY_SETTINGS --query "SELECT count() > 0 FROM (EXPLAIN $LAZY) WHERE explain ILIKE '%LazilyReadFromObjectStorage%'"

# The filesystem cache and the Parquet metadata cache keep serving the data files without the requests.
CACHE_SETTINGS="--filesystem_cache_name cache_for_readbigat --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 --use_columns_cache 0 --use_parquet_metadata_cache 1"
${CLICKHOUSE_CLIENT} $CACHE_SETTINGS --enable_filesystem_cache 1 --query_id "${Q}r1" --query "SELECT sum(c0) FROM t1"
${CLICKHOUSE_CLIENT} $CACHE_SETTINGS --enable_filesystem_cache 1 --query_id "${Q}r2" --query "SELECT sum(c0) FROM t1"
${CLICKHOUSE_CLIENT} $CACHE_SETTINGS --enable_filesystem_cache 1 --query_id "${Q}r3" --query "SELECT sum(c0), countIf(_etag != '') FROM t1"

${CLICKHOUSE_CLIENT} --query "SELECT countIf(_etag = '') FROM t0"
${CLICKHOUSE_CLIENT} --query "SELECT countIf(_time IS NULL) FROM t0"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t3 ENGINE = IcebergS3(s3_conn, filename = 'iceberg_negative_file_size_test')"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(c0) FROM t3"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        q,
        sumIf(head, files = 6) - sumIf(head, files = 3),
        sumIf(objects, files = 6) - sumIf(objects, files = 3),
        sumIf(manifests, files = 6) - sumIf(manifests, files = 3)
    FROM
    (
        SELECT
            splitByChar('_', substring(query_id, length('${Q}') + 1)) AS parts,
            parts[1] AS q,
            toUInt32(parts[2]) AS files,
            ProfileEvents['S3HeadObject'] AS head,
            ProfileEvents['IcebergMetadataReturnedObjectInfos'] AS objects,
            ProfileEvents['IcebergManifestFilesRead'] AS manifests
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND startsWith(query_id, '${Q}') AND match(substring(query_id, length('${Q}') + 1), '^[abcd]_[36]$')
    )
    GROUP BY q
    ORDER BY q
"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        anyIf(ProfileEvents['CachedReadBufferReadFromCacheBytes'], query_id = '${Q}r2') > 0,
        anyIf(ProfileEvents['ParquetMetadataCacheHits'], query_id = '${Q}r2') > 0,
        anyIf(ProfileEvents['S3HeadObject'], query_id = '${Q}r3') - anyIf(ProfileEvents['S3HeadObject'], query_id = '${Q}r2') = 6
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id IN ('${Q}r2', '${Q}r3')
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t0"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t2"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t3"
